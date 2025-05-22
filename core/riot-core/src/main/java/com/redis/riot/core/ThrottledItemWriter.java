package com.redis.riot.core;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import org.springframework.batch.item.*;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;

public class ThrottledItemWriter<T> implements ItemStreamWriter<T> {

    private static final Duration LIMIT_REFRESH_PERIOD = Duration.ofSeconds(1);

    private static final Duration TIMEOUT_DURATION = Duration.ofMinutes(1);

    private final ItemWriter<T> delegate;

    private final RateLimiter rateLimiter;

    public ThrottledItemWriter(ItemWriter<T> delegate, int opsPerSecond) {
        this(delegate, RateLimiter.of(ClassUtils.getShortName(ThrottledItemWriter.class), rateLimiterConfig(opsPerSecond)));
    }

    private static RateLimiterConfig rateLimiterConfig(int opsPerSecond) {
        return RateLimiterConfig.custom().limitForPeriod(opsPerSecond).limitRefreshPeriod(LIMIT_REFRESH_PERIOD)
                .timeoutDuration(TIMEOUT_DURATION) // Max wait time for permit
                .build();
    }

    public ThrottledItemWriter(ItemWriter<T> delegate, RateLimiter rateLimiter) {
        Assert.notNull(delegate, "Delegate must not be null");
        Assert.notNull(rateLimiter, "Rate limiter must not be null");
        this.delegate = delegate;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        if (delegate instanceof ItemStream) {
            ((ItemStream) delegate).open(executionContext);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) {
        if (delegate instanceof ItemStream) {
            ((ItemStream) delegate).update(executionContext);
        }
    }

    @Override
    public void close() {
        if (delegate instanceof ItemStream) {
            ((ItemStream) delegate).close();
        }
    }

    @Override
    public void write(Chunk<? extends T> items) throws Exception {
        rateLimiter.acquirePermission(items.size());
        delegate.write(items);
    }

}
