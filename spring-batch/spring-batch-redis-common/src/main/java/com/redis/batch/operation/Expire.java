package com.redis.batch.operation;

import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.redis.batch.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Expire<K, V, T> extends AbstractWriteOperation<K, V, T> {

    private ToLongFunction<T> ttlFunction = t -> 0;

    public Expire(Function<T, K> keyFunction) {
        super(keyFunction);
    }

    public void setTtl(long millis) {
        setTtlFunction(t -> millis);
    }

    public void setTtlFunction(ToLongFunction<T> function) {
        this.ttlFunction = function;
    }

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return BatchUtils.execute(commands, items, this::execute);
    }

    @SuppressWarnings("rawtypes")
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        long ttl = ttlFunction.applyAsLong(item);
        if (ttl > 0) {
            return commands.pexpire(key(item), ttl);
        }
        return NOOP_REDIS_FUTURE;
    }

}
