package com.redis.riot;

import com.redis.testcontainers.RedisServer;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

@EnabledOnOs(OS.LINUX)
class REStack extends RiotTests {

	private static final RedisServer source = RedisContainerFactory.enterprise();
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
