--[[
  Move next job to be processed to active, lock it and fetch its data. The job
  may be delayed, in that case we need to move it to the delayed set instead.

  This operation guarantees that the worker owns the job during the locks
  expiration time. The worker is responsible of keeping the lock fresh
  so that no other worker picks this job again.

  Input:
      KEYS[1] wait key
      KEYS[2] active key
      KEYS[3] priority key
      KEYS[4] active event key
      KEYS[5] stalled key

      -- Rate limiting
      KEYS[6] rate limiter key
      KEYS[7] delayed key

      --
      KEYS[8] drained key

      --
      KEYS[9] throttles key
      KEYS[10] throttle counts key

      ARGV[1] key prefix
      ARGV[2] lock token
      ARGV[3] lock duration in milliseconds
      ARGV[4] timestamp
      ARGV[5] optional jobid

      ARGV[6] optional jobs per time unit (rate limiter)
      ARGV[7] optional time unit (rate limiter)
      ARGV[8] optional do not do anything with job if rate limit hit
]]

local jobId
local rcall = redis.call

if(ARGV[5] ~= "") then
  jobId = ARGV[5]

  -- clean stalled key
  rcall("SREM", KEYS[5], jobId)
else
  -- move from wait to active
  jobId = rcall("RPOPLPUSH", KEYS[1], KEYS[2])

  -- keep popping elements from wait and see if the throttle is < throttle_max
  -- if it is, then move to active, and break
  -- if not, then move to delayed

  -- while true do
  --   local jobId = rcall("RPOP", KEYS[1])
  --   if not jobId then break end

  --   local throttle_id = rcall("HGET", jobId, "throttle_id")
  --   local throttle_max = rcall("HGET", KEYS[9], throttle_id)

  --   -- TODO: implement throttle_current_counts
  --   local throttle_current = rcall("HGET", <throttle-current_counts>, throttle_id)
  --   if throttle_current < throttle_max then
  --     -- push to active, then break
  --     rcall("LPUSH", KEYS[2], jobId)
  --     break
  --   else
  --     -- push to delayed, and continue looping
  --     rcall("LPUSH", KEYS[7], jobId)      
  --   end
  -- end

end

if jobId then
  local jobKey = ARGV[1] .. jobId
  local throttle_id = rcall("HGET", jobKey, "throttle_id")
  local rateLimiterKey
  local maxJobs
  -- local throttleCountKey = throttle_id .. ":count"
  
  -- Check if we need to use throttling
  if throttle_id then
    rateLimiterKey = throttle_id .. ":count"
    maxJobs = tonumber(rcall("HGET", KEYS[9], throttle_id))
  else
    rateLimiterKey = KEYS[6]
    maxJobs = tonumber(ARGV[6])
  end
  
  -- Check if we need to perform rate limiting.
  if maxJobs then
    local jobCounter = rcall("INCR", rateLimiterKey)
    local bounceBack = ARGV[8]
    
    -- throttle rate limit hit
    -- rate limit hit
    if jobCounter > maxJobs then
      if bounceBack == 'false' then
        local exceedingJobs = jobCounter - maxJobs
        local delay = tonumber(rcall("PTTL", rateLimiterKey)) + ((exceedingJobs - 1) * ARGV[7]) / maxJobs
        local timestamp = delay + tonumber(ARGV[4])

        -- put job into delayed queue
        rcall("ZADD", KEYS[7], timestamp * 0x1000 + bit.band(jobCounter, 0xfff), jobId)
        rcall("PUBLISH", KEYS[7], timestamp)
      end
      -- remove from active queue
      rcall("LREM", KEYS[2], 1, jobId)
      return
    else
      if tonumber(jobCounter) == 1 then
        rcall("PEXPIRE", rateLimiterKey, 10000)
        -- rcall("PEXPIRE", rateLimiterKey, ARGV[7])
      end
    end
  end

  local jobKey = ARGV[1] .. jobId
  local lockKey = jobKey .. ':lock'

  -- get a lock
  rcall("SET", lockKey, ARGV[2], "PX", ARGV[3])

  rcall("ZREM", KEYS[3], jobId) -- remove from priority
  rcall("PUBLISH", KEYS[4], jobId)
  rcall("HSET", jobKey, "processedOn", ARGV[4])

  return {rcall("HGETALL", jobKey), jobId} -- get job data
else
  rcall("PUBLISH", KEYS[8], "")
end
