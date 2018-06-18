local key = KEYS[1]
local price = ARGV[1]
local size = tonumber(ARGV[2])
local absolute_value_is_set = tonumber(redis.call("HSETNX", key, price, math.abs(size))) == 1

if absolute_value_is_set then
    redis.call("ZADD", (key .. ":prices"), price, price)
else
    redis.call("HINCRBYFLOAT", key, price, size)
end

return absolute_value_is_set
