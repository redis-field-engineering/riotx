package com.redis.riot.replicate;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyValueEvent;
import com.redis.riot.core.RiotUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import java.time.Duration;
import java.time.Instant;

public class ReplicateMetricsWriteListener<K> implements ItemWriteListener<KeyValueEvent<K>> {

    public static final String METRICS_PREFIX = "riotx.replication.";

    public static final String LAG_TIMER_NAME = METRICS_PREFIX + "lag";

    public static final String LAG_TIMER_DESCRIPTION = "Replication end-to-end latency";

    private MeterRegistry meterRegistry = Metrics.globalRegistry;

    @Override
    public void afterWrite(Chunk<? extends KeyValueEvent<K>> items) {
        onItems(items, true);
    }

    @Override
    public void onWriteError(Exception exception, Chunk<? extends KeyValueEvent<K>> items) {
        onItems(items, false);
    }

    private void onItems(Chunk<? extends KeyValueEvent<K>> items, boolean success) {
        for (KeyValueEvent<K> item : items) {
            onItem(item, success);
        }
    }

    private void onItem(KeyValueEvent<K> item, boolean success) {
        Duration lag = Duration.between(item.getTimestamp(), Instant.now());
        Tags tags = BatchUtils.tags(item.getEvent(), item.getType(), success);
        RiotUtils.latencyTimer(meterRegistry, LAG_TIMER_NAME, LAG_TIMER_DESCRIPTION, lag, tags);
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

}
