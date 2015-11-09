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

local _mod = {}

local ffi = require("ffi")

local bit = require("bit")
local IPv4 = require("proto_ipv4")
local UDP = require("proto_udp")
local Logger = require("logger")

local MDPPktDef = require("mdp/pkt_def")

-- Packet header details
local kMDP_PKT_HDR_PTR = ffi.typeof("struct mdp_packet_header *")
local kMDP_PKT_HDR_LEN = ffi.sizeof("struct mdp_packet_header")

-- Message header details
local kMDP_MESSAGE_HDR_PTR = ffi.typeof("struct mdp_message_header *")
local kMDP_MESSAGE_HDR_LEN = ffi.sizeof("struct mdp_message_header")

-- Security Status Message
local kMDP_SECURITY_STATUS_PTR = ffi.typeof("struct mdp_security_status *")

-- Instrument Definition: Future
local kMDP_INSTRUMENT_DEFINITION_FUTURE = ffi.typeof("struct mdp_instrument_definition_future *")

-- char ptr for pointer arithmetic
local kUINT8_PTR = ffi.typeof("uint8_t *")

local kMDP_MD_INCREMENTAL_REFRESH_BOOK_PTR = ffi.typeof("struct mdp_md_incremental_refresh_book *")

local kMDP_VERSION_SUPPORTED = 5

-- Update action constants
local kUPDATE_ACTION_NEW = 0
local kUPDATE_ACTION_CHANGE = 1
local kUPDATE_ACTION_DELETE = 2
local kUPDATE_ACTION_DELETE_THRU = 3
local kUPDATE_ACTION_DELETE_FROM = 4
local kUPDATE_ACTION_OVERLAY = 5

-- Template name declarations
local kCHANNEL_RESET                          = 4
local kADMIN_HEARTBEAT                        = 12
local kADMIN_LOGOUT                           = 16
local kMD_INSTRUMENT_DEFINITION_FUTURE        = 27
local kMD_INSTURMENT_DEFINITION_SPREAD        = 29
local kSECURITY_STATUS                        = 30
local kMD_INCREMENTAL_REFRESH_BOOK            = 32
local kMD_INCREMENTAL_REFRESH_DAILY_STATS     = 33
local kMD_INCREMENTAL_REFRESH_LIMITS_BANDING  = 34
local kMD_INCREMENTAL_REFRESH_SESSION_STATS   = 35
local kMD_INCREMENTAL_REFRESH_TRADE           = 36
local kMD_INCREMENTAL_REFRESH_VOLUME          = 37
local kSNAPSHOT_FULL_REFRESH                  = 38
local kQUOTE_REQUEST                          = 39
local kMD_INSTRUMENT_DEFINITION_OPTION        = 41
local kMD_INCREMENTAL_REFRESH_TRADE_SUMMARY   = 42

local MarketDataHandler = {}
MarketDataHandler.__index = MarketDataHandler

local function cast_payload(msg_hdr, payload_type)
  return ffi.cast(payload_type, ffi.cast(kUINT8_PTR, msg_hdr) + kMDP_MESSAGE_HDR_LEN)
end

local function handle_channel_reset(mdstate, msg_hdr)
  Logger.debug("RESET", "Channel Reset Occurred")
  return true
end

local function handle_heartbeat(mdstate, msg_hdr)
  -- Eat the heartbeats (we don't care for them, alas)
  return true
end

local function handle_admin_logout(mdstate, msg_hdr)
  Logger.debug("LOGOUT", "Admin logout occurred")
  return true
end

local function handle_instrument_definition(mdstate, msg_hdr)
  local idf = cast_payload(msg_hdr, kMDP_INSTRUMENT_DEFINITION_FUTURE)

  Logger.info("INSTRUMENT-DEFINITION-FUTURE", "Symbol: %s Group: %s Asset: %s Currency: %s SettlCurrency: %s Units of %d %s",
    ffi.string(idf.symbol, 20),
    ffi.string(idf.security_group, 6),
    ffi.string(idf.asset, 6),
    ffi.string(idf.currency, 3),
    ffi.string(idf.settl_currency, 3),
    tonumber(idf.unit_of_measure_quantity),
    ffi.strong(idf.unit_of_measure, 30))

  return true
end

local function handle_incremental_refresh_limits_banding(mdstate, msg_hdr)
  return true
end

local function handle_incremental_refresh_trade(mdstate, msg_hdr)
  return true
end

local function handle_incremental_refresh_session_stats(mdstate, msg_hdr)
  return true
end

local function handle_incremental_refresh_trade_summary(mdstate, msg_hdr)
  return true
end

local function handle_incremental_refresh_volume(mdstate, msg_hdr)
  return true
end

local function handle_incremental_refresh_daily_stats(mdstate, msg_hdr)
  return true
end

local security_status = {
  [2] = "TradingHalt",
  [4] = "Close",
  [15] = "NewPriceIndication",
  [17] = "ReadyToTrade",
  [18] = "NotAvailableForTrading",
  [20] = "UnknownorInvalid",
  [21] = "PreOpen",
  [24] = "PreCross",
  [25] = "Cross",
  [26] = "PostClose",
  [103] = "NoChange",
}

local function handle_security_status(mdstate, msg_hdr)
  local ss = cast_payload(msg_hdr, kMDP_SECURITY_STATUS_PTR)

  Logger.debug("SECURITY-STATUS", "[%8d] Security Group='%s', Asset='%s' Status=%s (%d)",
    ss.security_id,
    ffi.string(ss.security_group, 6),
    ffi.string(ss.asset, 6),
    security_status[ss.security_trading_status],
    ss.security_trading_status)

  mdstate.on_symbol_activation(mdstate, ss)

  return true
end

local function handle_incremental_refresh(mdstate, msg_hdr)
  local md_inc = cast_payload(msg_hdr, kMDP_MD_INCREMENTAL_REFRESH_BOOK_PTR)

  if md_inc.md_entries_size.num_in_group > 0 then
    for i = 0,md_inc.md_entries_size.num_in_group-1 do
      local entry = md_inc.md_entries[i]
      if entry.md_update_action == kUPDATE_ACTION_NEW then
        mdstate.on_new_price_level(mdstate, entry)
      elseif entry.md_update_action == kUPDATE_ACTION_CHANGE then
        mdstate.on_update_price_level(mdstate, entry)
      elseif entry.md_update_action == kUPDATE_ACTION_DELETE then
        mdstate.on_delete_price_level(mdstate, entry)
      elseif entry.md_update_action == kUPDATE_ACTION_DELETE_THRU then
        mdstate.on_delete_entire_side(mdstate, entry)
      elseif entry.md_update_action == kUPDATE_ACTION_DELETE_FROM then
        mdstate.on_delete_price_level_from(mdstate, entry)
      else
        Logger.warning("INCREMENTAL-REFRESH-UNKNOWN-ACTION", "MDUpdateAction = %d is not known or supported.",
          entry.md_update_action)
      end
    end
  end

  return true
end

local function handle_refresh_volume(mdstate, msg_hdr)
  --Logger.debug("VOLUME-REFRESH", "Volume refresh message")
end

local function handle_snapshot_full(mdstate, msg_hdr)
  Logger.debug("SNAPSHOT-FULL", "Full book snapshot")
  return true
end

local function handle_unhandled(mdstate, msg_hdr)
  Logger.debug("UNHANDLED", "<Template %d>", msg_hdr.template_id)
  return true
end

local IPv4_as_ipv4_header = IPv4.as_ipv4_header
local IPv4_get_payload = IPv4.get_payload
local UDP_as_udp_header = UDP.as_udp_header
local UDP_get_payload = UDP.get_payload

-- Given the payload from a link layer packet (i.e. an Ethernet frame), return the first
-- MDP 3.0 message header in the packet, and the associated timestamps and sequence number
local function get_first_message_header(payload)
  local udp_payload, udp_length = UDP_get_payload(UDP_as_udp_header(IPv4_get_payload(IPv4_as_ipv4_header(payload))))

  local pkt_hdr = ffi.cast(kMDP_PKT_HDR_PTR, udp_payload)
  local first_msg_hdr = ffi.cast(kMDP_MESSAGE_HDR_PTR, ffi.cast(kUINT8_PTR, pkt_hdr) + kMDP_PKT_HDR_LEN)

  -- Return the first message header and the length of the payload
  return first_msg_hdr, udp_length - kMDP_PKT_HDR_LEN, pkt_hdr.msg_seq_num, pkt_hdr.sending_time
end

-- Given the current message header and the remaining bytes, return the next message header
local function get_next_message_header(cur_msg_hdr, bytes_remain)
  if bytes_remain <= cur_msg_hdr.msg_size then
    return nil, 0
  else
    return ffi.cast(kMDP_MESSAGE_HDR_PTR, ffi.cast(kUINT8_PTR, cur_msg_hdr) + cur_msg_hdr.msg_size), bytes_remain - cur_msg_hdr.msg_size
  end
end

-- Overridable market data handler callback for when a new price level is created
function MarketDataHandler.on_new_price_level(md, msg)
  return
end

-- Overridable market data handler callback for when a price level is updated
function MarketDataHandler.on_update_price_level(md, msg)
  return
end

-- Overridable market data handler callback for when a price level is deleted
function MarketDataHandler.on_delete_price_level(md, msg)
  return
end

-- Handle a delete-from (upwards)
function MarketDataHandler.on_delete_price_level_from(md, msg)
  return
end

-- Handle clearing an entire book side
function MarketDataHandler.on_delete_entire_side(md, msg)
  return
end

-- Called by the parent script to force events to execute on a per-message basis.
function MarketDataHandler.on_message(self, ll_payload)
  local last_seq_num = self._last_seq_num
  local hdr, len, seq_num, time = get_first_message_header(ll_payload)
  local nr_messages = 0

  -- TODO: create a queue we can use to reorder messages if they arrive out of order
  if last_seq_num + 1 ~= seq_num then
    if last_seq_num > seq_num then
      Logger.debug("SEQ-NUM-BACKWARDS", "BACKWARDS! SeqNum = %d, LastSeqNum = %d", seq_num, last_seq_num)
    elseif last_seq_num ~= 0 then
      Logger.debug("SEQ-NUM-SKIP", "SKIP! SeqNum = %d, LastSeqNum = %d jumps %d", seq_num, last_seq_num, seq_num - last_seq_num)
    end
  end

  -- Keep this sequence number around
  self._last_seq_num = seq_num

  -- Process each message within the packet
  while hdr ~= nil do
    nr_messages = nr_messages + 1
    -- Check if we support this schema version
    if hdr.version ~= kMDP_VERSION_SUPPORTED then
      Logger.error("UNSUPP-MDP3-SCHEMA-VERSION", "The active MDP schema version %d is unsupported, aborting.", hdr.version)
      break
    end
 
--    if nil ~= templateHandlers[hdr.template_id] then
--      templateHandlers[hdr.template_id].action(self, hdr)
--    else
--      Logger.error("UNKNOWN-MDP3-TEMPLATE", "Unknown template <%d> encountered.", hdr.template_id)
--    end

    if hdr.template_id == kMD_INCREMENTAL_REFRESH_BOOK then
      handle_incremental_refresh(self, hdr)
    elseif hdr.template_id == kSECURITY_STATUS then
      handle_security_status(self, hdr)
    elseif hdr.template_id == kMD_INSTRUMENT_DEFINITION_FUTURE then
      handle_instrument_definition(self, hdr)
    elseif hdr.template_id == kADMIN_HEARTBEAT then
     handle_heartbeat(self, hdr)
    elseif hdr.template_id == kSNAPSHOT_FULL_REFRESH then
      handle_snapshot_full(self, hdr)      
    elseif hdr.template_id == kQUOTE_REQUEST then
      Logger.debug("TEMPLATE", "QUOTE_REQUEST")
    elseif hdr.template_id == kMD_INCREMENTAL_REFRESH_TRADE_SUMMARY then
      handle_incremental_refresh_trade_summary(self, hdr)
    elseif hdr.template_id == kMD_INCREMENTAL_REFRESH_VOLUME then
      handle_incremental_refresh_volume(self, hdr)
    elseif hdr.template_id == kMD_INCREMENTAL_REFRESH_SESSION_STATS then
      handle_incremental_refresh_session_stats(self, hdr)
    elseif hdr.template_id == kMD_INCREMENTAL_REFRESH_DAILY_STATS then
      handle_incremental_refresh_daily_stats(self, hdr)
    elseif hdr.template_id == kMD_INCREMENTAL_REFRESH_LIMITS_BANDING then
      handle_incremental_refresh_limits_banding(self, hdr)
    else
      handle_unhandled(self, hdr)
    end

    -- Get the next message header, if applicable
    hdr, len = get_next_message_header(hdr, len)
  end

  return nr_messages
end

function _mod.new_market_data_handler()
  local self = setmetatable({}, MarketDataHandler)
  self._last_seq_num = 0
  return self
end

return _mod

