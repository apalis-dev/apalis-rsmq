-- KEYS[1] = zset  
-- KEYS[2] = attrs hash  
-- KEYS[3] = QUEUES set
-- ARGV[1] = queue name
-- Removes the zset and the attrs hash. Because message bodies, :rc, :fr
-- and our :meta fields all live in the attrs hash, deleting that hash
-- removes every :meta field with it, so no per-message cleanup is needed.

if redis.call("EXISTS", KEYS[2]) == 0 then
  return redis.error_reply("QueueNotFound")
end
redis.call("DEL", KEYS[1], KEYS[2])
redis.call("SREM", KEYS[3], ARGV[1])
return 1
