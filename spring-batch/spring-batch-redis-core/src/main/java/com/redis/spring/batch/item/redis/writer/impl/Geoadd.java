package com.redis.spring.batch.item.redis.writer.impl;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.Collection;
import java.util.List;
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
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
        return BatchUtils.execute(commands, items, this::execute);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        return commands.geoadd(key(item), args(item), value(item).toArray(new GeoValue[0]));
    }

}
