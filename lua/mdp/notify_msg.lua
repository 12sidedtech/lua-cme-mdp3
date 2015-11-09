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

-- For the fast_notify_header
local IpqueueManager = require("ipqueue_manager")

-- Definitions for the notification packet to be put on the output IPqueue
-- for market data
ffi.cdef[[
  struct fn_mdu_price_level {
    int32_t price;
    int32_t size;
    int32_t nr_orders;
  } __attribute__((__packed__));

  struct fn_market_data_update {
    struct fast_notify_header hdr;
    int32_t symbol;
    int8_t nr_levs;
    int8_t flags;
    struct fn_mdu_price_level levels[3];
  } __attribute__((__packed__));
]]

