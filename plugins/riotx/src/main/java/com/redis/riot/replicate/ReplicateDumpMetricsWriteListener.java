package com.redis.riot.replicate;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyDumpEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import java.time.Duration;
import java.time.Instant;

public class ReplicateDumpMetricsWriteListener<K> implements ItemWriteListener<KeyDumpEvent<K>> {

    public static final String BYTES_COUNTER_NAME = ReplicateMetricsWriteListener.METRICS_PREFIX + "bytes";

    public static final String BYTES_COUNTER_DESCRIPTION = "Number of bytes replicated from source to target";

    private MeterRegistry meterRegistry = Metrics.globalRegistry;

    @Override
    public void afterWrite(Chunk<? extends KeyDumpEvent<K>> items) {
        onItems(items, true);
    }

    @Override
    public void onWriteError(Exception exception, Chunk<? extends KeyDumpEvent<K>> items) {
        onItems(items, false);
    }

    private void onItems(Chunk<? extends KeyDumpEvent<K>> items, boolean success) {
        for (KeyDumpEvent<K> item : items) {
            onItem(item, success);
        }
    }

    private void onItem(KeyDumpEvent<K> item, boolean success) {
        Duration lag = Duration.between(item.getTimestamp(), Instant.now());
        Tags tags = BatchUtils.tags(item.getEvent(), item.getType(), success);
        if (item.getDump() != null) {
            Counter bytes = Counter.builder(BYTES_COUNTER_NAME).description(BYTES_COUNTER_DESCRIPTION).tags(tags)
                    .register(meterRegistry);
            bytes.increment(item.getDump().length);
        }
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

}
