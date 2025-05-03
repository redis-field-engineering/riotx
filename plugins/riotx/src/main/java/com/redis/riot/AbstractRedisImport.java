package com.redis.riot;

import com.redis.riot.core.RedisContext;
import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisImport extends AbstractImport {

	@ArgGroup(exclusive = false, heading = "Redis options%n")
	private SingleRedisArgs redisArgs = new SingleRedisArgs();

	@Override
	protected RedisContext targetRedisContext() {
		return redisArgs.redisContext();
	}

	public SingleRedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(SingleRedisArgs clientArgs) {
		this.redisArgs = clientArgs;
	}

}
