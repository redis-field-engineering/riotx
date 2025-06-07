package com.redis.spring.batch.step;

import org.springframework.batch.core.step.factory.FaultTolerantStepFactoryBean;

import java.time.Duration;

public class FlushingFaultTolerantStepFactoryBean<T, S> extends FaultTolerantStepFactoryBean<T, S> {

    private static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

    private Duration idleTimeout;

    @Override
    protected FlushingFaultTolerantStepBuilder<T, S> createBuilder(String name) {
        FlushingFaultTolerantStepBuilder<T, S> flushingStepBuilder = new FlushingFaultTolerantStepBuilder<>(
                super.createBuilder(name));
        flushingStepBuilder.flushInterval(flushInterval);
        flushingStepBuilder.idleTimeout(idleTimeout);
        return flushingStepBuilder;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(Duration flushInterval) {
        this.flushInterval = flushInterval;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

}
