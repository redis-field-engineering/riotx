package com.redis.riot.replicate;

import java.time.Duration;
import java.time.Instant;

import com.redis.riot.core.RiotUtils;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.observability.BatchMetrics;
import org.springframework.batch.item.Chunk;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyValue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;

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
        Duration lag = Duration.between(item.getTime(), Instant.now());
        Tags tags = BatchUtils.tags(item, status);
        RiotUtils.latencyTimer(meterRegistry, LAG_TIMER_NAME, LAG_TIMER_DESCRIPTION, lag, tags);
        if (item.getMemoryUsage() > 0) {
            Counter bytes = Counter.builder(BYTES_COUNTER_NAME).description(BYTES_COUNTER_DESCRIPTION).tags(tags)
                    .register(meterRegistry);
            bytes.increment(item.getMemoryUsage());
        }
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

}
