package com.redis.riotx;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.observability.BatchMetrics;
import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.item.redis.common.KeyValue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

public class ReplicateMetricsWriteListener<K> implements ItemWriteListener<KeyValue<K>> {

	public static final String METRICS_PREFIX = "riotx.replication.";
	public static final String LAG_TIMER_NAME = METRICS_PREFIX + "lag";
	public static final String LAG_TIMER_DESCRIPTION = "Replication end-to-end latency";
	public static final String BYTES_COUNTER_NAME = METRICS_PREFIX + "bytes";
	public static final String BYTES_COUNTER_DESCRIPTION = "Number of bytes replicated from source to target";

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
		RiotxMetrics.replication(meterRegistry, LAG_TIMER_NAME, LAG_TIMER_DESCRIPTION, BYTES_COUNTER_NAME,
				BYTES_COUNTER_DESCRIPTION, item, status);
	}

	public void setMeterRegistry(MeterRegistry registry) {
		this.meterRegistry = registry;
	}

}
