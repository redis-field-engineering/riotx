package com.redis.riot;

import com.redis.riot.core.RedisContext;
import com.redis.riot.core.RedisContextFactory;
import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisImport extends AbstractImport {

	@ArgGroup(exclusive = false, heading = "Redis options%n")
	private RedisArgs redisArgs = new RedisArgs();

	@Override
	protected RedisContext targetRedisContext() {
		return RedisContextFactory.create(redisArgs.getUri(), redisArgs);
	}

	public RedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(RedisArgs clientArgs) {
		this.redisArgs = clientArgs;
	}

}
