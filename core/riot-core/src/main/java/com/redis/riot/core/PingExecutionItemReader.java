package com.redis.riot.core;

import com.redis.spring.batch.item.AbstractCountingItemReader;
import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.util.ClassUtils;

public class PingExecutionItemReader extends AbstractCountingItemReader<PingExecution> {

	private final RedisCommands<String, String> redisCommands;

	public PingExecutionItemReader(RedisCommands<String, String> redisCommands) {
		this.redisCommands = redisCommands;
	}

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

	@Override
	protected PingExecution doRead() throws Exception {
		return new PingExecution().reply(redisCommands.ping());
	}

}
