package com.redis.spring.batch.common;

import com.redis.spring.batch.item.AbstractCountingPollableItemReader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueItemReader<T> extends AbstractCountingPollableItemReader<T> {

    private final BlockingQueue<T> queue;

    public QueueItemReader(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    @Override
    protected void doOpen() throws Exception {
        // do nothing
    }

    @Override
    protected void doClose() throws Exception {
        // do close
    }

    @Override
    protected T doPoll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

}
