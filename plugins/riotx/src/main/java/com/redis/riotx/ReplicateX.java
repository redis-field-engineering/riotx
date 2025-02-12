package com.redis.riotx;

import java.io.IOException;

import org.springframework.util.StringUtils;

import com.redis.riot.RedisContext;
import com.redis.riot.Replicate;
import com.redis.riot.core.RiotException;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.RedisInfo;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "replicate", aliases = "sync", description = "Replicate a Redis database into another Redis database.")
public class ReplicateX extends Replicate {

	@ArgGroup(exclusive = false, heading = "Metrics options%n")
	private MetricsArgs metricsArgs = new MetricsArgs();

	@Override
	protected void initialize() {
		super.initialize();
		try {
			metricsArgs.configureMetrics();
		} catch (IOException e) {
			throw new RiotException("Could not initialize metrics", e);
		}
	}

	@Override
	protected RedisItemReader<byte[], byte[]> reader() {
		RedisItemReader<byte[], byte[]> reader = super.reader();
		reader.addItemReadListener(new ReplicateMetricsReadListener<>());
		return reader;
	}

	@Override
	protected void configureTargetRedisWriter(RedisItemWriter<?, ?, ?> writer) {
		writer.getRedisSupportCheck().getConsumers().add(this::unsupportedRedis);
		super.configureTargetRedisWriter(writer);
	}

	private void unsupportedRedis(RedisInfo info) {
		throw new UnsupportedOperationException(message(info));
	}

	private String message(RedisInfo info) {
		if (StringUtils.hasLength(info.getOS())) {
			return info.getOS();
		}
		return info.getServerName();
	}

	@Override
	protected Step<KeyValue<byte[]>, KeyValue<byte[]>> step() {
		Step<KeyValue<byte[]>, KeyValue<byte[]>> step = super.step();
		step.writeListener(new ReplicateMetricsWriteListener<>());
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
