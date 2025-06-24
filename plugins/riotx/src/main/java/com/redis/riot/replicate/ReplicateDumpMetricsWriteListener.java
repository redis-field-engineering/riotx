package com.redis.riot.replicate;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyValueEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import java.time.Duration;
import java.time.Instant;

public class ReplicateDumpMetricsWriteListener<K> implements ItemWriteListener<KeyValueEvent<K>> {

    public static final String BYTES_COUNTER_NAME = ReplicateMetricsWriteListener.METRICS_PREFIX + "bytes";

    public static final String BYTES_COUNTER_DESCRIPTION = "Number of bytes replicated from source to target";

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
        if (item.getValue() != null) {
            Counter bytes = Counter.builder(BYTES_COUNTER_NAME).description(BYTES_COUNTER_DESCRIPTION).tags(tags)
                    .register(meterRegistry);
            bytes.increment(((byte[])item.getValue()).length);
        }
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

}
