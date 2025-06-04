package com.redis.riot;

import com.redis.testcontainers.RedisServer;

class StackFileTests extends FileTests {

	private static final RedisServer redis = RedisContainerFactory.redis();

	private static final RedisServer target = RedisContainerFactory.redis();

	@Override
	protected RedisServer getRedisServer() {
		return redis;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return target;
	}

}
