package com.redis.riotx;

import java.io.IOException;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.util.StringUtils;

import com.redis.riot.RedisContext;
import com.redis.riot.Replicate;
import com.redis.riot.core.RiotException;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.RedisInfo;
import com.redis.spring.batch.item.redis.writer.impl.Del;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", aliases = "sync", description = "Replicate a Redis database into another Redis database.")
public class ReplicateX extends Replicate {

	@ArgGroup(exclusive = false, heading = "Metrics options%n")
	private MetricsArgs metricsArgs = new MetricsArgs();

	@Option(names = "--remove-source-keys", description = "Delete keys from source after they have been successfully replicated.")
	private boolean removeSourceKeys;

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
	protected ItemWriter<KeyValue<byte[]>> replicateWriter() {
		ItemWriter<KeyValue<byte[]>> writer = super.replicateWriter();
		if (removeSourceKeys) {
			log.info("Adding source delete writer to replicate writer");
			RedisItemWriter<byte[], byte[], KeyValue<byte[]>> sourceDelete = new RedisItemWriter<>(
					ByteArrayCodec.INSTANCE, new Del<>(KeyValue::getKey));
			configureSourceRedisWriter(sourceDelete);
			return new CompositeItemWriter<>(writer, sourceDelete);
		}
		return writer;
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
	protected Step<KeyValue<byte[]>, KeyValue<byte[]>> replicateStep() {
		Step<KeyValue<byte[]>, KeyValue<byte[]>> step = super.replicateStep();
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
