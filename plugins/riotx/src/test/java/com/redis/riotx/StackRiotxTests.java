package com.redis.riotx;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class StackRiotxTests extends RiotxTests {

	private final RedisStackContainer source = RedisContainerFactory.stack();

	private final RedisStackContainer target = RedisContainerFactory.stack();

	@Override
	protected RedisServer getRedisServer() {
		return source;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return target;
	}

}
