package com.redis.spring.batch.step;

import com.redis.spring.batch.item.PollableItemReader;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.factory.FaultTolerantStepFactoryBean;

import java.time.Duration;

public class FlushingStepFactoryBean<T, S> extends FaultTolerantStepFactoryBean<T, S> {

    private static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

    private static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;

    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

    private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

    @Override
    protected SimpleStepBuilder<T, S> createBuilder(String name) {
        if (getItemReader() instanceof PollableItemReader) {
            FlushingFaultTolerantStepBuilder<T, S> flushingStepBuilder = new FlushingFaultTolerantStepBuilder<>(
                    super.createBuilder(name));
            flushingStepBuilder.flushInterval(flushInterval);
            flushingStepBuilder.idleTimeout(idleTimeout);
            return flushingStepBuilder;
        }
        return super.createBuilder(name);
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
