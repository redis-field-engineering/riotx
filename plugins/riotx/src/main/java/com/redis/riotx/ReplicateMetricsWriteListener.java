package com.redis.riotx;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.observability.BatchMetrics;
import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.item.redis.common.KeyValue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class ReplicateMetricsWriteListener<K> implements ItemWriteListener<KeyValue<K>> {

	public static final String METRICS_PREFIX = "riotx.replication.";
	public static final String LAG_TIMER_NAME = METRICS_PREFIX + "lag";
	public static final String LAG_TIMER_DESCRIPTION = "Replication latency";
	public static final String BYTES_COUNTER_NAME = METRICS_PREFIX + "bytes";
	public static final String BYTES_COUNTER_DESCRIPTION = "Number of bytes replicated";

	private MeterRegistry meterRegistry = Metrics.globalRegistry;

	@Override
	public void afterWrite(Chunk<? extends KeyValue<K>> items) {
		onItems(items, BatchMetrics.STATUS_SUCCESS);
	}

	@Override
	public void onWriteError(Exception exception, Chunk<? extends KeyValue<K>> items) {
		onItems(items, BatchMetrics.STATUS_FAILURE);
	}

	private void onItems(Chunk<? extends KeyValue<K>> items, String status) {
		for (KeyValue<K> item : items) {
			onItem(item, status);
		}
	}

	private void onItem(KeyValue<K> item, String status) {
		Iterable<Tag> tags = tags(item, status);
		Timer lag = Timer.builder(LAG_TIMER_NAME).description(LAG_TIMER_DESCRIPTION).tags(tags).register(meterRegistry);
		lag.record(System.currentTimeMillis() - item.getTimestamp(), TimeUnit.MILLISECONDS);
		if (item.getMemoryUsage() > 0) {
			Counter bytes = Counter.builder(BYTES_COUNTER_NAME).description(BYTES_COUNTER_DESCRIPTION).tags(tags)
					.register(meterRegistry);
			bytes.increment(item.getMemoryUsage());
		}
	}

	private Iterable<Tag> tags(KeyValue<K> item, String status) {
		Tags tags = Tags.of("event", item.getEvent(), "status", status);
		if (item.getType() == null) {
			return tags;
		}
		return tags.and("type", item.getType());
	}

	public void setMeterRegistry(MeterRegistry registry) {
		this.meterRegistry = registry;
	}

}
