package com.redis.spring.batch.item.redis.reader;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.MeterRegistry;

public class KeyScanEventItemReader<K, V> extends KeyEventItemReader<K, V> {

	private final KeyScanItemReader<K, V> scanReader;

	public KeyScanEventItemReader(AbstractRedisClient client, RedisCodec<K, V> codec,
			KeyScanItemReader<K, V> scanReader) {
		super(client, codec);
		this.scanReader = scanReader;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		scanReader.open(executionContext);
		super.open(executionContext);
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		scanReader.update(executionContext);
		super.update(executionContext);
	}

	@Override
	public void close() throws ItemStreamException {
		super.close();
		scanReader.close();
	}

	@Override
	protected KeyEvent<K> doPoll(long timeout, TimeUnit unit) throws Exception {
		if (queue.isEmpty()) {
			KeyEvent<K> keyEvent = scanReader.read();
			if (keyEvent != null) {
				return keyEvent;
			}
		}
		return super.doPoll(timeout, unit);
	}

	@Override
	public void setMeterRegistry(MeterRegistry registry) {
		scanReader.setMeterRegistry(registry);
		super.setMeterRegistry(registry);
	}

}
