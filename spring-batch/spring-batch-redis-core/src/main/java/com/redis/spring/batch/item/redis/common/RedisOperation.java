package com.redis.spring.batch.item.redis.common;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public interface RedisOperation<K, V, I, O> {

    @SuppressWarnings("rawtypes")
    RedisFuture NOOP_REDIS_FUTURE = new NoopRedisFuture();

    @SuppressWarnings("unchecked")
    static <T> RedisFuture<T> noopRedisFuture() {
        return (RedisFuture<T>) NOOP_REDIS_FUTURE;
    }

    List<RedisFuture<O>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends I> items);

}
