package com.redis.riotx;

import org.springframework.batch.core.Job;

import com.redis.riot.AbstractExportCommand;
import com.redis.riot.RedisArgs;
import com.redis.riot.RedisContext;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "stats", description = "Show Redis database statistics.")
public class Stats extends AbstractExportCommand {

	@ArgGroup(exclusive = false, heading = "Redis options%n")
	private RedisArgs redisArgs = new RedisArgs();

	@Override
	protected RedisContext sourceRedisContext() {
		throw new UnsupportedOperationException();
	}

	@Override
	protected Job job() {
		return job(step());
	}

	private Step<KeyValue<String>, ?> step() {
		StatsItemWriter topKeysWriter = new StatsItemWriter();
		// return step(topKeysWriter).executionListener(new
		// TopKeysStepExecutionListener(topKeysWriter));
		throw new UnsupportedOperationException();
	}

	@Override
	protected boolean shouldShowProgress() {
		return false;
	}

}
