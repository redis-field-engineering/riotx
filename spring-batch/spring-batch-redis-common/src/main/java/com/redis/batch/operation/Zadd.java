package com.redis.batch.operation;

import com.redis.batch.BatchUtils;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

public class Zadd<K, V, T> extends AbstractValueWrite<K, V, Collection<ScoredValue<V>>, T> {

    private Function<T, ZAddArgs> argsFunction = t -> null;

    public Zadd(Function<T, K> keyFunction, Function<T, Collection<ScoredValue<V>>> valueFunction) {
        super(keyFunction, valueFunction);
    }

    public void setArgs(ZAddArgs args) {
        this.argsFunction = t -> args;
    }

    public void setArgsFunction(Function<T, ZAddArgs> function) {
        this.argsFunction = function;
    }

    @Override
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return execute(commands, items, this::execute);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        K key = key(item);
        ZAddArgs args = argsFunction.apply(item);
        Collection<ScoredValue<V>> collection = value(item);
        if (BatchUtils.isEmpty(collection)) {
            return NOOP_REDIS_FUTURE;
        }
        ScoredValue[] values = collection.toArray(new ScoredValue[0]);
        return commands.zadd(key, args, values);
    }

}
