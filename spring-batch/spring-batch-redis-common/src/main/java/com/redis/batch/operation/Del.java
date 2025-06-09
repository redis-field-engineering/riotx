package com.redis.batch.operation;

import java.util.List;
import java.util.function.Function;

import com.redis.batch.BatchUtils;
import com.redis.batch.RedisOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Del<K, V, T> implements RedisOperation<K, V, T, Object> {

    private final Function<T, K> keyFunction;

    public Del(Function<T, K> keyFunction) {
        this.keyFunction = keyFunction;
    }

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return BatchUtils.execute(commands, items, this::execute);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        return commands.del(keyFunction.apply(item));
    }

}
