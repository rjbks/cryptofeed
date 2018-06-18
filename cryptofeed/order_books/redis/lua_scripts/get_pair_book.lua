local key = KEYS[1]
local bid = ARGV[1] or 'bid'
local ask = ARGV[2] or 'ask'
local start_range = tonumber(ARGV[3] or 0)
local end_range = tonumber(ARGV[4] or -1)
local bidkey = key .. ':' .. bid
local askkey = key .. ':' .. ask
local bidpriceskey = bidkey .. ':prices'
local askpriceskey = askkey .. ':prices'
local bidprices = redis.call("ZREVRANGE", bidpriceskey, start_range, end_range)
local askprices = redis.call("ZRANGE", askpriceskey, start_range, end_range)

local function call_in_chunks(command, key, args)
    -- lua arg limit is 8000, so we may need to chunk calls for large order books
    local step = 7000
    local aggregated = {}
    for i = 1, #args, step do
        for _, val in ipairs(redis.call(command, key, unpack(args, i, math.min(i + step - 1, #args)))) do
            table.insert(aggregated, val)
        end
    end
    return aggregated
end

local function zip(prices, sizes)
    local new = {}
    for i, price in ipairs(prices) do
        new[i] = {price, sizes[i]}
    end
    return new
end

local bidsizes = call_in_chunks("HMGET", bidkey, bidprices)
local asksizes = call_in_chunks("HMGET", askkey, askprices)

local bids = zip(bidprices, bidsizes)
local asks = zip(askprices,asksizes)

return {bids, asks}
