package com.redis.batch.operation;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.redis.batch.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Hset<K, V, T> extends AbstractValueWriteOperation<K, V, Map<K, V>, T> {

    public Hset(Function<T, K> keyFunction, Function<T, Map<K, V>> valueFunction) {
        super(keyFunction, valueFunction);
    }

    @Override
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return execute(commands, items, this::execute);
    }

    @SuppressWarnings("rawtypes")
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        Map<K, V> value = value(item);
        if (BatchUtils.isEmpty(value)) {
            return NOOP_REDIS_FUTURE;
        }
        return commands.hset(key(item), value);
    }

}
