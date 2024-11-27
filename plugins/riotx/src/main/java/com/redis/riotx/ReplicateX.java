package com.redis.riotx;

import java.io.IOException;

import com.redis.riot.RedisContext;
import com.redis.riot.Replicate;
import com.redis.riot.core.RiotInitializationException;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;

public class ReplicateX extends Replicate {

	@ArgGroup(exclusive = false, heading = "Metrics options%n")
	private MetricsArgs metricsArgs = new MetricsArgs();

	@Override
	protected void initialize() throws RiotInitializationException {
		super.initialize();
		try {
			metricsArgs.configureMetrics();
		} catch (IOException e) {
			throw new RiotInitializationException("Could not initialize metrics", e);
		}
	}

	@Override
	protected void configureTargetRedisWriter(RedisItemWriter<?, ?, ?> writer) {
		writer.setSupportStrategy(new ProtectedRedisSupportStrategy());
		super.configureTargetRedisWriter(writer);
	}

	@Override
	protected Step<KeyValue<byte[]>, KeyValue<byte[]>> step() {
		Step<KeyValue<byte[]>, KeyValue<byte[]>> step = super.step();
		ReplicateMetricsWriteListener<byte[]> readWriteListener = new ReplicateMetricsWriteListener<>();
		step.writeListener(readWriteListener);
		return step;
	}

	@Override
	protected RedisContext sourceRedisContext() {
		RedisContext context = super.sourceRedisContext();
		metricsArgs.configure(context);
		return context;
	}

	@Override
	protected RedisContext targetRedisContext() {
		RedisContext context = super.targetRedisContext();
		metricsArgs.configure(context);
		return context;
	}

}
