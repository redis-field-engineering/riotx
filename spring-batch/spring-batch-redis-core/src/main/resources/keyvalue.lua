local function now()
  local time = redis.call('TIME')
  return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end

local function struct (key, t)
  if t == 'hash' then return redis.call('HGETALL', key)
  elseif t == 'ReJSON-RL' then return redis.call('JSON.GET', key)
  elseif t == 'list' then return redis.call('LRANGE', key, 0, -1)
  elseif t == 'set' then return redis.call('SMEMBERS', key)
  elseif t == 'stream' then return redis.call('XRANGE', key, '-', '+')
  elseif t == 'string' then return redis.call('GET', key)
  elseif t == 'TSDB-TYPE' then return redis.call('TS.RANGE', key, '-', '+')
  elseif t == 'zset' then return redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
  end
  return nil
end

--[[
    KEYS:
      1. Key for which to fetch TTL, mem usage, type, and value
--    ARGV:
--      1. mode: value format
--           DUMP (default): DUMP <key>
--           STRUCT        : data-structure specific command (string -> GET, hash -> HGETALL, ...)
--           else          : no value
--      2. limit: memory usage check mode (default: 0)
--           0  : skip memory usage check (fastest)
--          >0  : enforce max memory usage
--          -1  : always read memory usage but do not enforce
--      3. samples: number of nested samples for MEMORY USAGE (optional)
--]]
local key = KEYS[1]
local mode = ARGV[1] or "DUMP"
local limit = tonumber(ARGV[2] or "0")
local samples = tonumber(ARGV[3] or "0")
local ttl = redis.call('PTTL', key)
if ttl >= 0 then
  ttl = now() + ttl
end
local mem_usage = 0
local key_type = nil
local value = nil
if ttl ~= -2 then
  if limit ~= 0 then
    if samples > 0 then
      mem_usage = redis.call('MEMORY', 'USAGE', key, 'SAMPLES', samples) or 0
    else
      mem_usage = redis.call('MEMORY', 'USAGE', key) or 0
    end
  end
  -- Get key type
  local type_result = redis.call('TYPE', key)
  if type(type_result) == "table" and type_result.ok then
    key_type = type_result.ok
  else
    key_type = type_result
  end
  if limit < 0 or mem_usage <= limit then
    if mode == 'DUMP' then
      value = redis.call('DUMP', key)
    elseif mode == 'STRUCT' then
      value = struct(key, key_type)
    end
  end
end
return { ttl, mem_usage, key_type, value }
