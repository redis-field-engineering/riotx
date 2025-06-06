package com.redis.spring.batch.step;

import org.springframework.batch.core.step.factory.SimpleStepFactoryBean;

import java.time.Duration;

public class FlushingStepFactoryBean<T, S> extends SimpleStepFactoryBean<T, S> {

    public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

    private Duration idleTimeout;

    @Override
    protected FlushingStepBuilder<T, S> createBuilder(String name) {
        FlushingStepBuilder<T, S> flushingStepBuilder = new FlushingStepBuilder<>(super.createBuilder(name));
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
