package com.redis.riot.replicate;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyTtlTypeEvent;
import com.redis.riot.core.RiotUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.springframework.batch.core.ItemReadListener;

import java.time.Duration;
import java.time.Instant;

public class ReplicateMetricsReadListener<K> implements ItemReadListener<KeyTtlTypeEvent<K>> {

    public static final String METRICS_PREFIX = "riotx.replication.read.";

    public static final String LAG_TIMER_NAME = METRICS_PREFIX + "latency";

    public static final String LAG_TIMER_DESCRIPTION = "Replication read latency";

    private MeterRegistry meterRegistry = Metrics.globalRegistry;

    @Override
    public void afterRead(KeyTtlTypeEvent<K> item) {
        Duration latency = Duration.between(Instant.now(), item.getTimestamp());
        RiotUtils.latencyTimer(meterRegistry, LAG_TIMER_NAME, LAG_TIMER_DESCRIPTION, latency,
                BatchUtils.tags(item.getEvent(), item.getType(), true));
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

}
