package com.redis.riot.core;

public class RedisMemcachedContext {

    private RedisContext redisContext;

    private MemcachedContext memcachedContext;

    public boolean isRedis() {
        return redisContext != null;
    }

    public boolean isMemcached() {
        return memcachedContext != null;
    }

    public RedisContext getRedisContext() {
        return redisContext;
    }

    public MemcachedContext getMemcachedContext() {
        return memcachedContext;
    }

    public void setRedisContext(RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    public void setMemcachedContext(MemcachedContext memcachedContext) {
        this.memcachedContext = memcachedContext;
    }

}
