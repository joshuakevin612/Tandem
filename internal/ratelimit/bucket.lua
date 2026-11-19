-- KEYS[1] bucket key; ARGV[1] rate (tokens/sec), ARGV[2] burst.
-- Returns {allowed, retry_ms}. Time comes from Redis TIME so replicas agree.
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local h = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(h[1])
local ts = tonumber(h[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end
local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(burst, tokens + elapsed * rate / 1000)

local allowed = 0
local retry = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = math.ceil((1 - tokens) * 1000 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
-- ~2x the time to refill a full bucket, at least 1s.
local ttl = math.ceil(2 * burst * 1000 / rate)
if ttl < 1000 then ttl = 1000 end
redis.call('PEXPIRE', KEYS[1], ttl)
return {allowed, retry}
