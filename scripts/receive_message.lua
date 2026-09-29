-- KEYS[1] = zset  
-- KEYS[2] = attrs hash
-- ARGV[1] = hidden_ms (-1 => queue default)


local t = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)

local vt = redis.call("HGET", KEYS[2], "vt")
if not vt then
    return redis.error_reply("QueueNotFound")
end
local hidden_ms = tonumber(ARGV[1])
if hidden_ms < 0 then
    hidden_ms = tonumber(vt) * 1000
end

local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", now_ms, "LIMIT", 0, 1)
if #ids == 0 then
    return {}
end
local id = ids[1]

redis.call("ZADD", KEYS[1], now_ms + hidden_ms, id)
redis.call("HINCRBY", KEYS[2], "totalrecv", 1)

local msg = redis.call("HGET", KEYS[2], id)
local rc = redis.call("HINCRBY", KEYS[2], id .. ":rc", 1)
local fr
if rc == 1 then
    fr = now_ms
    redis.call("HSET", KEYS[2], id .. ":fr", fr)
else
    fr = redis.call("HGET", KEYS[2], id .. ":fr")
end
local meta = redis.call("HGET", KEYS[2], id .. ":meta")
if not meta then
    meta = ""
end
return {id, msg, rc, fr, meta}
