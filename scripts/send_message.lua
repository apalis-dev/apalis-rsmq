
-- KEYS[1] = rsmq:q (zset)
-- KEYS[2] = rsmq:q:Q (attrs hash)
-- ARGV[1] = id  
-- ARGV[2] = message
-- ARGV[3] = delay_s  
-- ARGV[4] = meta ("" for none)
-- ARGV[5] = realtime("1"/"0")
-- ARGV[6] = queue name (for PUBLISH channel)

local t = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)

local q = redis.call("HMGET", KEYS[2], "maxsize", "delay")
if not q[1] then
    return redis.error_reply("QueueNotFound")
end
local maxsize = tonumber(q[1])
if maxsize ~= -1 and #ARGV[2] > maxsize then
    return redis.error_reply("MessageTooLong")
end

local delay_ms = tonumber(ARGV[3])
if delay_ms < 0 then
    delay_ms = tonumber(q[2]) * 1000
end

redis.call("ZADD", KEYS[1], now_ms + delay_ms, ARGV[1])
redis.call("HSET", KEYS[2], ARGV[1], ARGV[2])
if ARGV[4] ~= "" then
    redis.call("HSET", KEYS[2], ARGV[1] .. ":meta", ARGV[4])
end
redis.call("HINCRBY", KEYS[2], "totalsent", 1)
if ARGV[5] == "1" then
    local n = redis.call("ZCARD", KEYS[1])
    redis.call("PUBLISH", ARGV[6] and (ARGV[7] .. ":rt:" .. ARGV[6]) or "", n)
end
return ARGV[1]
