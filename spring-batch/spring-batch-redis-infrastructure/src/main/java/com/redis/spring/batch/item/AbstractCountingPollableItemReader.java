package com.redis.spring.batch.item;

import org.springframework.batch.item.ItemCountAware;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public abstract class AbstractCountingPollableItemReader<T> extends AbstractCountingItemReader<T>
        implements PollableItemReader<T> {

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(Long.MAX_VALUE);

    private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

    @Override
    protected T doRead() throws Exception {
        return poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws Exception {
        if (getCurrentItemCount() >= getMaxItemCount()) {
            return null;
        }
        setCurrentItemCount(getCurrentItemCount() + 1);
        T item = doPoll(timeout, unit);
        if (item instanceof ItemCountAware) {
            ((ItemCountAware) item).setItemCount(getCurrentItemCount());
        }
        return item;
    }

    protected abstract T doPoll(long timeout, TimeUnit unit) throws Exception;

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
    }

}
