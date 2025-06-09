package com.redis.batch.operation;

import com.redis.batch.BatchUtils;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.time.Instant;
import java.util.List;
import java.util.function.Function;

public class ExpireAt<K, V, T> extends AbstractWriteOperation<K, V, T> {

    private Function<T, Instant> timestampFunction = t -> null;

    public ExpireAt(Function<T, K> keyFunction) {
        super(keyFunction);
    }

    public void setTimestamp(Instant instant) {
        setTimestampFunction(t -> instant);
    }

    public void setTimestampFunction(Function<T, Instant> function) {
        this.timestampFunction = function;
    }

    private Instant instant(T item) {
        return timestampFunction.apply(item);
    }

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return BatchUtils.execute(commands, items, this::execute);
    }

    @SuppressWarnings("rawtypes")
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        Instant instant = instant(item);
        if (instant == null) {
            return NOOP_REDIS_FUTURE;
        }
        return commands.pexpireat(key(item), instant);
    }

}
