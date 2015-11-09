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

local ffi = require("ffi")

ffi.cdef[[
  struct mdp_packet_header {
    uint32_t msg_seq_num;
    uint64_t sending_time;
  } __attribute__((__packed__));

  struct mdp_message_header {
    uint16_t msg_size;
    uint16_t block_length;
    uint16_t template_id;
    uint16_t schema_id;
    uint16_t version;
  } __attribute__((__packed__));

  typedef int64_t mdp_price_null_t;

  struct mdp_group_size {
    uint16_t block_length;
    uint8_t num_in_group;
  } __attribute__((__packed__));

  struct mdp_maturity_month_year {
    uint16_t year;
    uint8_t month;
    uint8_t day;
    uint8_t week;
  } __attribute__((__packed__));

  struct mdp_instrument_definition_future {
    uint8_t match_event_indicator;
    uint32_t tot_num_reports;
    uint8_t security_update_action;
    uint64_t last_update_time;
    uint8_t md_security_trading_status;
    int16_t appl_id;
    uint8_t market_segment_id;
    uint8_t underlying_product;
    uint32_t security_exchange;
    char security_group[6];
    char asset[6];
    char symbol[20];
    int32_t security_id;
    uint8_t security_id_source;
    char security_type[6];
    char cfi_code[6];
    char maturity_month_year[5];
    char currency[3];
    char settl_currency[3];
    uint8_t match_algorithm;
    uint32_t min_trade_vol;
    uint32_t max_trade_vol;
    int64_t min_price_increment;
    int64_t display_factor;
    uint8_t main_fraction;
    uint8_t sub_fraction;
    uint8_t price_display_format;
    char unit_of_measure[30];
    mdp_price_null_t unit_of_measure_quantity;
    mdp_price_null_t trading_reference_price;
    uint8_t settl_price_type;
    int32_t open_interest_qty;
    int32_t cleared_volume;
    mdp_price_null_t high_limit_price;
    mdp_price_null_t low_limit_price;
    mdp_price_null_t max_price_variation;
    int32_t decay_quantity;
    uint16_t decay_start_date;
    int32_t orig_contract_size;
    int32_t contract_mul;
    int8_t contract_multiplier_unit;
    int8_t flow_schedule_type;
    mdp_price_null_t min_price_increment_amount;
    int8_t user_defined_instrument;
  } __attribute__((__packed__));

  struct mdp_security_status {
    uint64_t transact_time;
    char security_group[6];
    char asset[6];
    int32_t security_id;
    uint16_t trade_date;
    uint8_t match_event_indicator;
    uint8_t security_trading_status;
    uint8_t halt_reason;
    uint8_t security_trading_event;
  } __attribute__((__packed__));

  struct mdp_md_incremental_refresh_book_md_entries {
    mdp_price_null_t md_entry_px;
    uint32_t md_entry_size;
    int32_t security_id;
    uint32_t rpt_seq;
    uint32_t number_of_orders;
    uint8_t md_price_level;
    uint8_t md_update_action;
    uint8_t md_entry_type;
    uint8_t __padding__[5];
  } __attribute__((__packed__));

  struct mdp_md_incremental_refresh_book {
    uint64_t transact_time;
    uint8_t match_event_indicator;
    uint16_t wtf;
    struct mdp_group_size md_entries_size;
    struct mdp_md_incremental_refresh_book_md_entries md_entries[];
  } __attribute__((__packed__));

]]

