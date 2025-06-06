package com.redis.riot;

import com.redis.testcontainers.RedisServer;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

@EnabledOnOs(OS.LINUX)
class StackREContainer extends RiotTests {

	private static final RedisServer source = RedisContainerFactory.redis();
	private static final RedisServer target = RedisContainerFactory.enterprise();

	@Override
	protected RedisServer getRedisServer() {
		return source;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return target;
	}

}
