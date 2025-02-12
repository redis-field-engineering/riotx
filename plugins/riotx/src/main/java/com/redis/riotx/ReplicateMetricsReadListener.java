package com.redis.riotx;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.observability.BatchMetrics;

import com.redis.spring.batch.item.redis.common.KeyValue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class ReplicateMetricsReadListener<K> implements ItemReadListener<KeyValue<K>> {

	public static final String METRICS_PREFIX = "riotx.replication.read.";
	public static final String LAG_TIMER_NAME = METRICS_PREFIX + "latency";
	public static final String LAG_TIMER_DESCRIPTION = "Replication read latency";
	public static final String BYTES_COUNTER_NAME = METRICS_PREFIX + "bytes";
	public static final String BYTES_COUNTER_DESCRIPTION = "Number of bytes read from source";

	private MeterRegistry meterRegistry = Metrics.globalRegistry;

	@Override
	public void afterRead(KeyValue<K> item) {
		Iterable<Tag> tags = tags(item, BatchMetrics.STATUS_SUCCESS);
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
