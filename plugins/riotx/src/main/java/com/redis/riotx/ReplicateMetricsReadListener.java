package com.redis.riotx;

import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.observability.BatchMetrics;

import com.redis.spring.batch.item.redis.common.KeyValue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

public class ReplicateMetricsReadListener<K> implements ItemReadListener<KeyValue<K>> {

	public static final String METRICS_PREFIX = "riotx.replication.read.";
	public static final String LAG_TIMER_NAME = METRICS_PREFIX + "latency";
	public static final String LAG_TIMER_DESCRIPTION = "Replication read latency";
	public static final String BYTES_COUNTER_NAME = METRICS_PREFIX + "bytes";
	public static final String BYTES_COUNTER_DESCRIPTION = "Number of bytes read from source";

	private MeterRegistry meterRegistry = Metrics.globalRegistry;

	@Override
	public void afterRead(KeyValue<K> item) {
		RiotxMetrics.replication(meterRegistry, LAG_TIMER_NAME, LAG_TIMER_DESCRIPTION, BYTES_COUNTER_NAME,
				BYTES_COUNTER_DESCRIPTION, item, BatchMetrics.STATUS_SUCCESS);
	}

	public void setMeterRegistry(MeterRegistry registry) {
		this.meterRegistry = registry;
	}

}
