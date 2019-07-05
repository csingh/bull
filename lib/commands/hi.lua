local rcall = redis.call
rcall("SET", "merchant1", '1')
rcall("SET", "merchant2", '2')
rcall("SET", "merchant3", '3')

return rcall("LRANGE" myotherlist 0 -1)
