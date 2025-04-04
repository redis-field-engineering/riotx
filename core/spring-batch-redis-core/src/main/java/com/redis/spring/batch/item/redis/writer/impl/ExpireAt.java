package com.redis.spring.batch.item.redis.writer.impl;

import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class ExpireAt<K, V, T> extends AbstractWriteOperation<K, V, T> {

    private ToLongFunction<T> timestampFunction = t -> 0;

    public ExpireAt(Function<T, K> keyFunction) {
        super(keyFunction);
    }

    public void setTimestamp(long epoch) {
        setTimestampFunction(t -> epoch);
    }

    public void setTimestampFunction(ToLongFunction<T> function) {
        this.timestampFunction = function;
    }

    private long ttl(T item) {
        return timestampFunction.applyAsLong(item);
    }

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
        return BatchUtils.execute(commands, items, this::execute);
    }

    @SuppressWarnings("rawtypes")
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        long ttl = ttl(item);
        if (ttl > 0) {
            return commands.pexpireat(key(item), ttl);
        }
        return NOOP_REDIS_FUTURE;
    }

}
