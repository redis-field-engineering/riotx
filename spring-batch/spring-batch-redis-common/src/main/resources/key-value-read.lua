-- Helper: Determine the Redis data structure type for the given key
local function get_type(key)
    local result = redis.call('TYPE', key)
    return type(result) == "table" and result.ok or result
end

local function get_dump(key)
    local ok, result = pcall(redis.call, 'DUMP', key)
    return ok and result or nil
end

-- Helper: Return a structured representation of the key based on its type
local function get_struct(key, t)
    local ok, result
    if t == 'hash' then
        ok, result = pcall(redis.call, 'HGETALL', key)
    elseif t == 'ReJSON-RL' then
        ok, result = pcall(redis.call, 'JSON.GET', key)
    elseif t == 'list' then
        ok, result = pcall(redis.call, 'LRANGE', key, 0, -1)
    elseif t == 'set' then
        ok, result = pcall(redis.call, 'SMEMBERS', key)
    elseif t == 'stream' then
        ok, result = pcall(redis.call, 'XRANGE', key, '-', '+')
    elseif t == 'string' then
        ok, result = pcall(redis.call, 'GET', key)
    elseif t == 'TSDB-TYPE' then
        ok, result = pcall(redis.call, 'TS.RANGE', key, '-', '+')
    elseif t == 'zset' then
        ok, result = pcall(redis.call, 'ZRANGE', key, 0, -1, 'WITHSCORES')
    else
        return nil
    end
    return ok and result or nil
end

-- Helper: Get memory usage for the given key
local function get_memory_usage(key, samples)
    local ok, result
    if samples > 0 then
        ok, result = pcall(redis.call, 'MEMORY', 'USAGE', key, 'SAMPLES', samples)
    else
        ok, result = pcall(redis.call, 'MEMORY', 'USAGE', key)
    end
    return ok and result or 0
end

-- Helper: Extract value based on mode
local function get_value(key, type, mode, limit, samples)
    if mode == 'none' then
        return nil
    end
    -- Check that the key exists (Type none means it doesn't exist)
    if type == 'none' then
        return nil
    end
    if limit > 0 then
        local mem = get_memory_usage(key, samples)
        if mem > limit then
            return nil
        end
    end
    if mode == 'dump' then
        return get_dump(key)
    end
    if mode == 'struct' then
        return get_struct(key, type)
    end
    return nil
end

-- KEYS[1]: target key
-- ARGV[1]: mode (dump, struct, or none)
-- ARGV[2]: limit (0: skip, >0: enforce, -1: inspect only)
-- ARGV[3]: samples (for MEMORY USAGE, optional)

local mode    = ARGV[1] or "none"
local limit   = tonumber(ARGV[2] or "0")
local samples = tonumber(ARGV[3] or "0")
local key     = KEYS[1]
local ttl     = redis.call('PTTL', key)
local type    = get_type(key)
local value   = get_value(key, type, mode, limit, samples)

return { ttl, type, value }
