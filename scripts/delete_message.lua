-- KEYS[1] = zset  
-- KEYS[2] = attrs hash   
-- ARGV[1] = id

local removed = redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", KEYS[2], ARGV[1], ARGV[1] .. ":rc", ARGV[1] .. ":fr", ARGV[1] .. ":meta")
return removed
