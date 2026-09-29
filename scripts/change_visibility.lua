-- KEYS[1] = zset  
-- KEYS[2] = attrs hash
-- ARGV[1] = id 
-- ARGV[2] = hidden_ms
-- Returns 1 if the message exists, 0 otherwise.

local t = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)

if redis.call("EXISTS", KEYS[2]) == 0 then
  return redis.error_reply("QueueNotFound")
end
if not redis.call("ZSCORE", KEYS[1], ARGV[1]) then return 0 end
redis.call("ZADD", KEYS[1], now_ms + tonumber(ARGV[2]), ARGV[1])
return 1
