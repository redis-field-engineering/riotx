package com.redis.batch.operation;

import com.redis.batch.RedisOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Restore<K, V, T> extends AbstractValueWriteOperation<K, V, byte[], T> {

    private final Predicate<T> deletePredicate = t -> false;

    private Function<T, Instant> ttlFunction = t -> null;

    public Restore(Function<T, K> keyFunction, Function<T, byte[]> valueFunction) {
        super(keyFunction, valueFunction);
    }

    public void setTtlFunction(Function<T, Instant> function) {
        this.ttlFunction = function;
    }

    private Instant ttl(T item) {
        return ttlFunction.apply(item);
    }

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        List<RedisFuture<Object>> futures = new ArrayList<>();
        futures.addAll(delete(commands, items));
        futures.addAll(restore(commands, items));
        return futures;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<RedisFuture<Object>> delete(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return (List) items.stream().filter(deletePredicate).map(keyFunction).map(commands::del).collect(Collectors.toList());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<RedisFuture<Object>> restore(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return (List) items.stream().filter(deletePredicate.negate()).map(t -> restore(commands, t))
                .collect(Collectors.toList());
    }

    private RedisFuture<String> restore(RedisAsyncCommands<K, V> commands, T item) {
        RestoreArgs args = new RestoreArgs().replace(true);
        Instant ttl = ttl(item);
        if (ttl != null) {
            args.absttl().ttl(ttl.toEpochMilli());
        }
        byte[] value = value(item);
        if (value == null) {
            return RedisOperation.noopRedisFuture();
        }
        return commands.restore(key(item), value, args);

    }

}
