package com.redis.batch;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public interface RedisBatchOperation<K, V, I, O> {

    @SuppressWarnings("rawtypes")
    RedisFuture NOOP_REDIS_FUTURE = new NoopRedisFuture();

    @SuppressWarnings("unchecked")
    static <T> RedisFuture<T> noopRedisFuture() {
        return (RedisFuture<T>) NOOP_REDIS_FUTURE;
    }

    List<? extends Future<O>> execute(RedisAsyncCommands<K, V> commands, List<? extends I> items);

    default List<Future<O>> execute(RedisAsyncCommands<K, V> commands, List<? extends I> items,
            RedisOperation<K, V, I, O> operation) {
        return items.stream().map(t -> operation.execute(commands, t)).collect(Collectors.toList());
    }

}
