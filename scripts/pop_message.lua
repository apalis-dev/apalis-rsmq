-- KEYS[1] = zset  
-- KEYS[2] = attrs hash
-- Same as receive, but the message is removed instead of hidden.
-- Returns { id, msg, rc, fr, meta } or {}.

local t = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)

if redis.call("EXISTS", KEYS[2]) == 0 then
  return redis.error_reply("QueueNotFound")
end

local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", now_ms, "LIMIT", 0, 1)
if #ids == 0 then return {} end
local id = ids[1]

redis.call("HINCRBY", KEYS[2], "totalrecv", 1)
local msg  = redis.call("HGET", KEYS[2], id)
local rc   = redis.call("HINCRBY", KEYS[2], id .. ":rc", 1)
local fr   = redis.call("HGET", KEYS[2], id .. ":fr")
if not fr then fr = now_ms end
local meta = redis.call("HGET", KEYS[2], id .. ":meta")
if not meta then meta = "" end

redis.call("ZREM", KEYS[1], id)
redis.call("HDEL", KEYS[2], id, id .. ":rc", id .. ":fr", id .. ":meta")
return { id, msg, rc, fr, meta }
