--
-- Lua Packet Processing - CME MDP 3.0 Example
--
-- Copyright (C) 2015 12Sided Technology, LLC
--
-- Permission is hereby granted, free of charge, to any person obtaining
-- a copy of this software and associated documentation files (the
-- "Software"), to deal in the Software without restriction, including
-- without limitation the rights to use, copy, modify, merge, publish,
-- distribute, sublicense, and/or sell copies of the Software, and to
-- permit persons to whom the Software is furnished to do so, subject to
-- the following conditions:
--
-- The above copyright notice and this permission notice shall be
-- included in all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
-- EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
-- MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
-- IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
-- CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
-- TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
-- SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
--
-- [ MIT license: http://www.opensource.org/licenses/mit-license.php ]
--

local Logger = require("logger")

-- The message format we'll emit
local NotifyMsg = require("mdp/notify_msg")
local IpqueueManager = require("ipqueue_manager")

local ffi = require("ffi")

local kFN_MARKET_DATA_UPDATE_PTR = ffi.typeof("struct fn_market_data_update *")
local kFN_MDU_PRICE_LEVEL = ffi.typeof("struct fn_mdu_price_level *")
local kFN_MDU_PATH_ID = 0x1337
local kFN_MDU_SIZE = ffi.sizeof("struct fn_market_data_update")

local kMDU_FLAG_BID   = 0x8
local kMDU_FLAG_ASK   = 0x4

local kBOOK_BID = 0
local kBOOK_ASK = 1

local Math_min = math.min

-- Cache output queue slot retrieval in an up-value
local OutputQueue_get_output_slot = IpqueueManager.OutputQueue.get_output_slot

local Book = {}
Book.__index = Book

--
-- Linked list functions
--

-- Initialize a new list item node inside the given table
local function list_init(itm)
  itm.next_itm = itm
  itm.prev_itm = itm
end

-- Internal function to splice two items together
local function _list_insert(itm, prev_itm, next_itm)
  prev_itm.next_itm = itm
  next_itm.prev_itm = itm
  itm.next_itm = next_itm
  itm.prev_itm = prev_itm
end

-- Insert new item after the given item
local function list_insert_after(itm, after)
  _list_insert(itm, after, after.next_itm)
end

-- Append the given item to the specified list
local function list_append(head, item)
  _list_insert(item, head, head.next_itm)
end

-- Prepend the given item to the specified list
local function list_prepend(head, item)
  _list_insert(item, head.prev_itm, head)
end

local function list_remove(itm)
  itm.prev_itm.next_itm = itm.next_itm
  itm.next_itm.prev_itm = itm.prev_itm

  -- poison the item
  itm.next_itm = nil
  itm.prev_itm = nil
end

local function list_head(head)
  if head.next_itm == head and head.prev_itm == head then
    return nil
  else
    return head.next_itm
  end
end

-- Check if the list is empty
local function list_empty(head)
  return head.next_item == head and head.prev_item == head
end

-- Walk the list, applying the given function (each) to every item found
local function list_walk(head, each)
  if list_empty(head) then
    return
  end

  local cur_item = head.next_itm
  local i = 0

  repeat
    each(i, cur_item)
    cur_item = cur_item.next_itm
    i = i + 1
  until cur_item == head
end

-- Create a new price level table
local function new_price_level(alloc)
  local lev = { price = 0.0, size = 0, nr_orders = 0, needs_update = false, next_itm = nil, prev_itm = nil }
  list_append(alloc, lev)
end

-- Allocate a new price levels allocator (basically a pool of pre-allocated price levels)
local function new_price_levels_alloc(nr_levels)
  local self = { nr_free_levels = nr_levels, next_itm = nil, prev_itm = nil }

  list_init(self)

  for i=1,nr_levels-1 do
    new_price_level(self)
  end

  return self
end

-- Free a given price level
local function price_level_free(alloc, level)
  list_prepend(alloc, level)
  alloc.nr_free_levels = alloc.nr_free_levels + 1
end

-- Allocate a new price level
local function price_level_alloc(alloc)
  local itm = list_head(alloc)

  if itm ~= nil then
    list_remove(itm)
    alloc.nr_free_levels = alloc.nr_free_levels - 1
  else
    Logger.debug("OUT-OF-MEM", "Out of memory, %d price levels free", alloc.nr_free_levels)
  end

  return itm
end

-- Create a new book
local function new_book(symbol, depth)
  local self = setmetatable({
                  bid = { nr_entries = 0 }, 
                  ask = { nr_entries = 0 }, 
                  price_levels = new_price_levels_alloc(depth * 3),
                  depth = depth,
                  symbol = symbol,
                  next_update = nil,
                }, Book)

  list_init(self.bid)
  list_init(self.ask)

  return self
end

-- Internal function to find a given price level
local function Book_find_price_level(book_side, level)
  local cur_lev = book_side

  for i=1,level do
    cur_lev = cur_lev.next_itm
    if cur_lev == book_side then
      Logger.debug("WRAP-AROUND", "Wrapped around, yikes.")
      cur_lev = nil
      break
    end
  end

  return cur_lev
end

-- Prune a book to the specified depth
local function Book_prune(alloc, book_side, max_depth)
  local nr_items = book_side.nr_entries
  local entries = book_side

  while nr_items > max_depth do
    local tail = entries.prev_itm
    list_remove(tail)
    price_level_free(alloc, tail)
    nr_items = nr_items - 1
  end

  book_side.nr_entries = nr_items
end

local function Book_update_side(symbol, side, flags, update_queue)
  if not side.needs_update then
    return
  end

  local update = ffi.cast(kFN_MARKET_DATA_UPDATE_PTR, OutputQueue_get_output_slot(update_queue, kFN_MDU_SIZE))

  update.hdr.length = kFN_MDU_SIZE
  update.hdr.path_id = kFN_MDU_PATH_ID
  update.symbol = symbol
  update.flags = flags

  update.nr_levs = Math_min(3, side.nr_entries)

  -- Walk the top of book to get the pricing we'll write out
  local cur_lev = side.next_itm
  for i=1,update.nr_levs do
    update.levels[i-1].price = cur_lev.price * 1000
    update.levels[i-1].size = cur_lev.size
    update.levels[i-1].nr_orders = cur_lev.nr_orders
    cur_lev = cur_lev.next_itm
  end

  --Logger.debug("POST-UPDATE", "[%8d] Posting update to book for side %d (%d levels)", update.symbol, flags, update.nr_levs)
end

function Book.post_update(self, update_queue)
  Book_update_side(self.symbol, self.bid, kMDU_FLAG_BID, update_queue)
  Book_update_side(self.symbol, self.ask, kMDU_FLAG_ASK, update_queue)
end

function Book.insert_new_price_level(self, side, price, size, nr_orders, level)
  local book_side = (side == kBOOK_ASK) and self.ask or self.bid

  --Logger.debug("PX-NEW", "[%8d] %d - %d@$%f (lev %d)", self.symbol, side, size, price, level)

  if level > book_side.nr_entries + 1 then
    --Logger.error("BAD-BOOK-STRUCTURE", "Requested book level %d be filled in, number of entries is %d, would leave gaps.", level, book_side.nr_entries)
    return false
  end

  local new_lev = price_level_alloc(self.price_levels)
  if nil == new_lev then
    error("Out of memory.")
  end

  new_lev.price = price
  new_lev.size = size
  new_lev.nr_orders = nr_orders

  local cur_lev = Book_find_price_level(book_side, level - 1)
  if cur_lev == nil then
    error(string.format("[%8d] Wraparound occurred in book (side=%d) walking, aborting. %d levels, asked for level %d", self.symbol, side, book_side.nr_entries, level))
  end

  list_insert_after(new_lev, cur_lev)
  book_side.nr_entries = book_side.nr_entries + 1

  if book_side.nr_entries > self.depth then
    Book_prune(self.price_levels, book_side, self.depth)
  end

  book_side.needs_update = level <= 3
  return book_side.needs_update
end

-- Remove the specified price level from the book
function Book.remove_price_level(self, side, level)
  local book_side = (side == kBOOK_ASK) and self.ask or self.bid

  --Logger.debug("PX-DEL", "[%8d] %d (lev %d of %d)", self.symbol, side, level, book_side.nr_entries)

  if book_side.nr_entries < level then
    return false
  end

  local cur_lev = Book_find_price_level(book_side, level)

  if cur_lev == nil then
    Logger.error("UNKNOWN-QUOTE", "[%8d] Could not remove book level %d (side = %d) (existing %d entries)", self.symbol, level, side, book_side.nr_entries)
    return false
  end

  list_remove(cur_lev)
  price_level_free(self.price_levels, cur_lev)
  book_side.nr_entries = book_side.nr_entries - 1
  book_side.needs_update = level <= 3
  return book_side.needs_update
end

-- Update the specified price level
function Book.update_price_level(self, side, price, size, nr_orders, level)
  local book_side = (side == kBOOK_ASK) and self.ask or self.bid
  --Logger.debug("PX-UPD", "[%8d] %d -> %d@%f (lev %d)", self.symbol, side, size, price, level)

  if level > book_side.nr_entries then
    --Logger.error("INVALID-PX-LEVEL", "Attempted to update a level that could not possibly exist: %d (havea %d)", level, book_side.nr_entries)
    return false
  end

  local cur_lev = Book_find_price_level(book_side, level)
  if cur_lev.price ~= price then
    Logger.error("INVALID-PX", "Current level price = %f, expected %f", cur_level.price, price)
    assert(cur_lev.price == price)
  end
  cur_lev.size = size
  cur_lev.nr_orders = nr_orders
  book_side.needs_update = level <= 3
end

-- Clear all price levels on one side of the book
function Book.clear_all_price_levels(self, side)
  local book_side = (side == kBOOK_ASK) and self.ask or self.bid

  if book_side.nr_entries == 0 then
    Logger.debug("WEIRD-BOOK-STATE", "Got clear-all for book that is empty?")
    return false
  end

  local cur_level = book_side.next_itm
  local alloc = self.price_levels

  while cur_level ~= book_side do
    local next_level = cur_level.next_itm
    list_remove(cur_level)
    price_level_free(alloc, cur_level)
    cur_level = next_level
    book_side.nr_entries = book_side.nr_entries - 1
  end

  book_side.needs_update = true
  return book_side.needs_update
end

-- Delete levels from 1 through levels from the specified book side
function Book.remove_price_levels_from(self, side, levels)
  local book_side = (side == kBOOK_ASK) and self.ask or self.bid

  Logger.debug("PX-DTH", "[%8d] Delete thru %d (have %d levels)", self.symbol, levels, book_side.nr_entries)

  if list_empty(book_side) then
    Logger.debug("NO-ITEMS", "Asked to remove first %d levels, there are no levels to remove", levels)
    return false
  end

  if levels > book_side.nr_entries then
    Logger.debug("NOT-ENOUGH-ITEMS", "There are %d items in the book and %d levels are to be removed", book_side.nr_entries, levels)
  end

  local cur_level = book_side.next_itm
  local rem = levels

  while cur_level ~= book_side and level > 0 do
    local next_level = cur_level.next_itm
    list_del(cur_level)
  end

  book_side.needs_update = true
  return book_side.needs_update
end

return {
  Book = Book,
  new_book = new_book
}

