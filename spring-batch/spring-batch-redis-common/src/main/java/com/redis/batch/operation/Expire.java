package com.redis.batch.operation;

import com.redis.batch.RedisBatchOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

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
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return items.stream().map(t -> execute(commands, t)).collect(Collectors.toList());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private RedisFuture<Object> execute(RedisAsyncCommands<K, V> commands, T item) {
        long ttl = ttlFunction.applyAsLong(item);
        if (ttl > 0) {
            return (RedisFuture) commands.pexpire(key(item), ttl);
        }
        return RedisBatchOperation.noopRedisFuture();
    }

}
