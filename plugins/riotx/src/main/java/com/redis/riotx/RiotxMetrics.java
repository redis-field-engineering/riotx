package com.redis.riotx;

import java.util.concurrent.TimeUnit;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class RiotxMetrics {

	public static void replication(MeterRegistry registry, String latencyName, String latencyDescription,
			String bytesName, String bytesDescription, KeyValue<?> item, String status) {
		Tags tags = BatchUtils.tags(item, status);
		Timer lag = Timer.builder(latencyName).description(latencyDescription).tags(tags).register(registry);
		lag.record(System.currentTimeMillis() - item.getTimestamp(), TimeUnit.MILLISECONDS);
		if (item.getMemoryUsage() > 0) {
			Counter bytes = Counter.builder(bytesName).description(bytesDescription).tags(tags).register(registry);
			bytes.increment(item.getMemoryUsage());
		}
	}

}
