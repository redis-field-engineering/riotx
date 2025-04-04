package com.redis.spring.batch.item.redis.writer.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Restore<K, V, T> extends AbstractValueWriteOperation<K, V, byte[], T> {

    private final Predicate<T> deletePredicate = t -> ttl(t) == KeyValue.TTL_NO_KEY;

    private ToLongFunction<T> ttlFunction = t -> 0;

    public Restore(Function<T, K> keyFunction, Function<T, byte[]> valueFunction) {
        super(keyFunction, valueFunction);
    }

    public void setTtlFunction(ToLongFunction<T> function) {
        this.ttlFunction = function;
    }

    private long ttl(T item) {
        return ttlFunction.applyAsLong(item);
    }

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
        List<RedisFuture<Object>> futures = new ArrayList<>();
        futures.addAll(delete(commands, items));
        futures.addAll(restore(commands, items));
        return futures;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<RedisFuture<Object>> delete(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
        return (List) BatchUtils.stream(items).filter(deletePredicate).map(keyFunction).map(commands::del)
                .collect(Collectors.toList());
    }

    private List<RedisFuture<Object>> restore(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
        return BatchUtils.stream(items).filter(deletePredicate.negate()).map(t -> restore(commands, t))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("rawtypes")
    private RedisFuture restore(RedisAsyncCommands<K, V> commands, T item) {
        RestoreArgs args = new RestoreArgs().replace(true);
        long ttl = ttl(item);
        if (ttl > 0) {
            args.absttl().ttl(ttl);
        }
        byte[] value = value(item);
        if (value == null) {
            return NOOP_REDIS_FUTURE;
        }
        return commands.restore(key(item), value, args);

    }

}
