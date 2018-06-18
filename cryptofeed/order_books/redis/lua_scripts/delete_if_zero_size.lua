local key = KEYS[1]
local price = ARGV[1]
local val = tonumber(redis.call("HGET", key, price))
local deleted = false

local function approx_zero(num)
    return -(1e-14) <= num and num <= (1e-14)
end

if type(val) == "number" and approx_zero(val) then
    redis.call("HDEL", key, price)
    redis.call("ZREM", (key .. ':prices'), price)
    deleted = true
end

return deleted
