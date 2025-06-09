package com.redis.batch;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;

public class NoopRedisFuture<T> extends CompletableFuture<T> implements RedisFuture<T> {

    public NoopRedisFuture() {
        complete(null);
    }

    @Override
    public String getError() {
        return "";
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return LettuceFutures.awaitAll(timeout, unit, this);
    }

}
