package com.redis.batch.operation;

import com.redis.batch.BatchUtils;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TsAdd<K, V, T> extends AbstractValueWriteOperation<K, V, Collection<Sample>, T> {

    private Function<T, AddOptions<K, V>> optionsFunction = t -> null;

    public TsAdd(Function<T, K> keyFunction, Function<T, Collection<Sample>> valueFunction) {
        super(keyFunction, valueFunction);
    }

    public void setOptions(AddOptions<K, V> options) {
        setOptionsFunction(t -> options);
    }

    public void setOptionsFunction(Function<T, AddOptions<K, V>> function) {
        this.optionsFunction = function;
    }

    private AddOptions<K, V> options(T item) {
        return optionsFunction.apply(item);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return (List) items.stream().flatMap(t -> execute(commands, t)).collect(Collectors.toList());
    }

    private Stream<RedisFuture<Long>> execute(RedisAsyncCommands<K, V> commands, T item) {
        Collection<Sample> samples = value(item);
        if (BatchUtils.isEmpty(samples)) {
            return Stream.empty();
        }
        RedisModulesAsyncCommands<K, V> modulesCommands = (RedisModulesAsyncCommands<K, V>) commands;
        K key = key(item);
        AddOptions<K, V> options = options(item);
        return samples.stream().map(t -> modulesCommands.tsAdd(key, t, options));
    }

}
