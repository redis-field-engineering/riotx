package com.redis.batch.operation;

import com.redis.batch.BatchUtils;
import com.redis.batch.RedisBatchOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractMemberWrite<K, V, T> extends AbstractValueWrite<K, V, Collection<V>, T> {

    protected AbstractMemberWrite(Function<T, K> keyFunction, Function<T, Collection<V>> valueFunction) {
        super(keyFunction, valueFunction);
    }

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return items.stream().map(t -> execute(commands, t)).collect(Collectors.toList());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private RedisFuture<Object> execute(RedisAsyncCommands<K, V> commands, T item) {
        Collection<V> value = value(item);
        if (BatchUtils.isEmpty(value)) {
            return RedisBatchOperation.noopRedisFuture();
        }
        return (RedisFuture) execute(commands, key(item), (V[]) value.toArray());
    }

    protected abstract RedisFuture<Long> execute(RedisAsyncCommands<K, V> commands, K key, V[] values);

}
