--[[
  Release lock

     Input:
        KEYS[1] 'lock',
        KEYS[2] throttleName,
      
        ARGV[1]  token
        ARGV[2]  lock duration in milliseconds,
      
      Output:
        "OK" if lock extented succesfully.
]]
local rcall = redis.call

if rcall("GET", KEYS[1]) == ARGV[1] then
  if rcall("DEL", KEYS[1])
    rcall("INCR", KEYS[2])
  return "OK"
else
  return 0
end
