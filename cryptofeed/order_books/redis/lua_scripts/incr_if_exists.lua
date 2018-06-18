local key = KEYS[1]
local price = ARGV[1]
local size = tonumber(ARGV[2])
local exists = tonumber(redis.call("HEXISTS", key, price)) > 0

if exists then
    redis.call("HINCRBYFLOAT", key, price, size)
end

return exists
