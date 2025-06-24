package com.redis.batch.operation;

import com.redis.batch.BatchUtils;
import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

public class Geoadd<K, V, T> extends AbstractValueWriteOperation<K, V, Collection<GeoValue<V>>, T> {

    private Function<T, GeoAddArgs> argsFunction = t -> null;

    public Geoadd(Function<T, K> keyFunction, Function<T, Collection<GeoValue<V>>> valueFunction) {
        super(keyFunction, valueFunction);
    }

    public void setArgs(GeoAddArgs args) {
        this.argsFunction = t -> args;
    }

    public void setArgsFunction(Function<T, GeoAddArgs> args) {
        this.argsFunction = args;
    }

    private GeoAddArgs args(T item) {
        return argsFunction.apply(item);
    }

    @Override
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return execute(commands, items, this::execute);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        return commands.geoadd(key(item), args(item), value(item).toArray(new GeoValue[0]));
    }

}
