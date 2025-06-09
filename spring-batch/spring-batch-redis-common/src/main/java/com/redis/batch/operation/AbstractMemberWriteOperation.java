package com.redis.batch.operation;

import com.redis.batch.BatchUtils;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public abstract class AbstractMemberWriteOperation<K, V, T> extends AbstractValueWriteOperation<K, V, Collection<V>, T> {

    protected AbstractMemberWriteOperation(Function<T, K> keyFunction, Function<T, Collection<V>> valueFunction) {
        super(keyFunction, valueFunction);
    }

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return BatchUtils.execute(commands, items, this::execute);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        Collection<V> value = value(item);
        if (BatchUtils.isEmpty(value)) {
            return NOOP_REDIS_FUTURE;
        }
        return execute(commands, key(item), (V[]) value.toArray());
    }

    protected abstract RedisFuture<Long> execute(RedisAsyncCommands<K, V> commands, K key, V[] values);

}
