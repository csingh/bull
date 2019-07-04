local rcall = redis.call

rcall("SET", KEYS[1], ARGV[2])

local greet = ARGV[1]
local name = rcall("GET", KEYS[1])

local result = greet.." "..name


return result.. 0x1000

