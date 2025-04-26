package com.redis.riot;

import java.time.Duration;

import com.redis.riot.core.RiotUtils;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.observability.BatchMetrics;

import com.redis.spring.batch.item.redis.common.BatchUtils;
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
        Duration latency = Duration.ofMillis(System.currentTimeMillis() - item.getTimestamp());
        RiotUtils.latencyTimer(meterRegistry, LAG_TIMER_NAME, LAG_TIMER_DESCRIPTION, latency,
                BatchUtils.tags(item, BatchMetrics.STATUS_SUCCESS));
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

}
