local key = KEYS[1]
local price = ARGV[1]
local decr_amount = -tonumber(ARGV[2])
local removed = false
local amount_left

local function approx_zero(num)
    return -(1e-14) <= num and num <= (1e-14)
end

if redis.call("HEXISTS", key, price) ~= 0 then
    amount_left = tonumber(redis.call("HINCRBYFLOAT", key, price, decr_amount))
    if type(amount_left) == "number" and approx_zero(amount_left) then
        redis.call("HDEL", key, price)
        redis.call("ZREM", (key .. ':prices'), price)
        removed = true
    end
end

return removed
