--[[
  // Change: making it less general by adding logic for it to grab global semaphore

  Takes a lock

     Input:
        KEYS[1] 'lock'
        KEYS[2] throttleName

        ARGV[1]  token
        ARGV[2]  lock duration in milliseconds
]]

if redis.call("GET", KEYS[2]) <= 0 then
  return 0
else 
  if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
     redis.call("DECR", KEYS[2])
     print("In TakeLock", KEYS[2])
    return 1
  else 
    return 0
  end
end
