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

local harv_ptr, conf_ptr = ...

local ffi = require("ffi")
local math = require("math")

local IpqueueManager = require("ipqueue_manager")
local HarvesterBase = require("harvester_base")
local Config = require("config")
local EchoTable = require("echo_table")
local Logger = require("logger")
local Endian = require("endianess");

local Ethernet = require("proto_ethernet")
local MDP3 = require("mdp/proto_mdp3_sbe")

local BookBuilder = require("mdp/book_builder")

-- local Trace = require("trace")
-- local Profiler = require("profiler")
--
ffi.cdef[[
  uint64_t tsl_get_clock_monotonic(void);
]]

local C = ffi.C

-- Set up the logger
Logger.set_log_level(Logger.SEV_DEBUG)
Logger.set_app_name("MDP-HANDLER")
Logger.info("STARTING", "Starting CME MDP 3.0 monitoring tools")

local harv = HarvesterBase.new(harv_ptr)
local cfg = Config.new(conf_ptr)
local queue_name = cfg:get_string("outputQueue")

if nil == queue_name then
  Logger.error("BAD-QUEUE-NAME", "need to specify an outputQueue for the MDP 3.0 harvester to output market data to")
end

local packet_queue = harv:open_packet_queue(conf_ptr)
local out_queue = IpqueueManager.open_output_queue(queue_name)

local OutputQueue_post = IpqueueManager.OutputQueue.post

local last_seq_num = 0

local mdp_hdlr = MDP3.new_market_data_handler()

-- Cache a few of the protocol API methods as up-values (simple optimization)
local Ethernet_as_etherframe = Ethernet.as_etherframe
local Ethernet_get_payload = Ethernet.get_payload
local Hdlr_on_message = mdp_hdlr.on_message
local PacketQueue_next_packet = packet_queue.next_packet
local BookBuilder_new_book = BookBuilder.new_book

-- Cache methods for the book as up-values (optimization)
local Book_insert_new_price_level = BookBuilder.Book.insert_new_price_level
local Book_remove_price_level = BookBuilder.Book.remove_price_level
local Book_update_price_level = BookBuilder.Book.update_price_level
local Book_update_clear_all_price_levels = BookBuilder.Book.clear_all_price_levels
local Book_remove_price_levels_from = BookBuilder.Book.remove_price_levels_from
local Book_post_update = BookBuilder.Book.post_update

local total_ns = 0
local nr_packets = 0
local min_ns = 0x7fffffff
local max_ns = 0
local nr_datagrams = 0

local _books = {}

local kBOOK_BID = 0
local kBOOK_ASK = 1

local books_pending_update = nil

local function append_update_queue(book)
  if nil ~= book.next_update then
    -- Already in the list
    return
  end

  book.next_update = books_pending_update
  books_pending_update = book
end

local function update_pending_books()
  while nil ~= books_pending_update do
    local cur_book = books_pending_update
    -- Force an update to the IPQueue
    Book_post_update(books_pending_update, out_queue)
    -- Remove from the pending queue
    books_pending_update = books_pending_update.next_update
    cur_book.next_update = nil
  end
end

local function get_book_by_id(symbol_id)
  if _books[symbol_id] == nil then
    --Logger.debug("NEW-BOOK", "Creating new book for symbol %d", symbol_id)
    _books[symbol_id] = BookBuilder_new_book(symbol_id, 12)
    return _books[symbol_id]
  else
    return _books[symbol_id]
  end
end

local function handle_new_price_level(md, entry)
  if entry.md_entry_type ~= 48 and entry.md_entry_type ~= 49 then
    return
  end

  local side = (entry.md_entry_type == 48) and kBOOK_BID or kBOOK_ASK
  local book = get_book_by_id(entry.security_id)

  if (Book_insert_new_price_level(book, side, tonumber(entry.md_entry_px)/10000000.0, entry.md_entry_size, entry.number_of_orders, entry.md_price_level)) then
    append_update_queue(book)
  end
end

local function handle_update_price_level(md, entry)
  if entry.md_entry_type ~= 48 and entry.md_entry_type ~= 49 then
    return
  end

  local side = (entry.md_entry_type == 48) and kBOOK_BID or kBOOK_ASK
  local book = get_book_by_id(entry.security_id)

  if (Book_update_price_level(book, side, tonumber(entry.md_entry_px)/10000000.0, entry.md_entry_size, entry.number_of_orders, entry.md_price_level)) then
    append_update_queue(book)
  end
end

local function handle_delete_price_level(md, entry)
  if entry.md_entry_type ~= 48 and entry.md_entry_type ~= 49 then
    return
  end

  local side = (entry.md_entry_type == 48) and kBOOK_BID or kBOOK_ASK
  local book = get_book_by_id(entry.security_id)

  if Book_remove_price_level(book, side, entry.md_price_level) then
    append_update_queue(book)
  end
end

local function handle_delete_price_levels_from(md, entry)
  local side = (entry.md_entry_type == 48) and kBOOK_BID or kBOOK_ASK
  local book = get_book_by_id(entry.security_id)
  if Book_remove_price_levels_from(book, side, entry.md_price_level) then
    append_update_queue(book)
  end
end

local function handle_delete_entire_side(md, entry)
  local side = (entry.md_entry_type == 48) and kBOOK_BID or kBOOK_ASK
  local book = get_book_by_id(entry.security_id)
  if Book_update_clear_all_price_levels(book, side) then
    append_update_queue(book)
  end
end

local function handle_symbol_activation(md, symbol_state)
  -- Add each book during market opening
  get_book_by_id(symbol_state.security_id)
end

-- Override default price level action dispatchers
mdp_hdlr.on_new_price_level = handle_new_price_level
mdp_hdlr.on_update_price_level = handle_update_price_level
mdp_hdlr.on_delete_price_level = handle_delete_price_level
mdp_hdlr.on_delete_price_level_from = handle_delete_price_levels_from
mdp_hdlr.on_delete_entire_side = handle_delete_entire_side
mdp_hdlr.on_symbol_activation = handle_symbol_activation

local Math_min = math.min
local Math_max = math.max
local Harv_is_done = harv.is_done
local last_event = 0

-- Trace.start("/tmp/mdp_trace.txt")
-- Profiler.start("fFl", "/tmp/mdp_profile.txt")

repeat
  local prec, payload = PacketQueue_next_packet(packet_queue, 100)

  if nil ~= prec then
--    local start_ns = C.tsl_get_clock_monotonic()

    nr_packets = nr_packets + Hdlr_on_message(mdp_hdlr, Ethernet_get_payload(Ethernet_as_etherframe(payload)))
    last_event = C.tsl_get_clock_monotonic()
--    nr_datagrams = nr_datagrams + 1

--    local delta = tonumber(C.tsl_get_clock_monotonic() - start_ns)

--    min_ns = Math_min(min_ns, delta)
--    max_ns = Math_max(max_ns, delta)

--    total_ns = total_ns + delta

--    if nr_packets > 1000 then
--      Logger.debug("STATS", "Total: %d ns Avg = %f ns min = %d ns max = %d ns %d messages (%d datagrams)", total_ns, total_ns/nr_packets, min_ns, max_ns, nr_packets, nr_datagrams)
--      total_ns = 0
--      nr_packets = 0
--      min_ns = 0x7fffffff
--      max_ns = 0
--      nr_datagrams = 0
--    end
  end

  update_pending_books()

  if tonumber(C.tsl_get_clock_monotonic()-last_event)/1000 > 500000 then
    -- Force the current slab to be posted to the output queue if we're idle for 500ms
    OutputQueue_post(out_queue)
  end
until Harv_is_done(harv)

-- Trace.stop()
-- Profiler.stop()

--Logger.debug("STATS", "Handled %d messages (%d datagrams)", nr_packets, nr_datagrams)

Logger.info("TERMINATING", "Terminating CME MDP3 Handler for multicast group x.x.x.x")

return 0

