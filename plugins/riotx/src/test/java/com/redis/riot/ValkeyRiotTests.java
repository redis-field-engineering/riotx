package com.redis.riot;

import com.redis.testcontainers.RedisServer;

public class ValkeyRiotTests extends RiotTests {

    private static final RedisServer source = RedisContainerFactory.valkey();

    private static final RedisServer target = RedisContainerFactory.redis();

    @Override
    protected RedisServer getRedisServer() {
        return source;
    }

    @Override
    protected RedisServer getTargetRedisServer() {
        return target;
    }

}
