package com.redis.batch.operation;

import com.redis.batch.RedisBatchOperation;
import com.redis.lettucemod.api.async.RediSearchAsyncCommands;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateResults;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

public class Aggregate<K, V, T> implements RedisBatchOperation<K, V, T, AggregateResults<K>> {

    private final Function<T, K> index;

    private final Function<T, V> query;

    private Function<T, AggregateOptions<K, V>> options = t -> null;

    private Function<T, List<V>> stringOptions = t -> Collections.emptyList();

    public Aggregate(Function<T, K> index, Function<T, V> query) {
        this.index = index;
        this.query = query;
    }

    @Override
    public List<? extends Future<AggregateResults<K>>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return execute(commands, items, this::execute);
    }

    @SuppressWarnings("unchecked")
    private RedisFuture<AggregateResults<K>> execute(RedisAsyncCommands<K, V> commands, T item) {
        RediSearchAsyncCommands<K, V> search = (RediSearchAsyncCommands<K, V>) commands;
        K indexKey = index.apply(item);
        V queryString = query.apply(item);
        AggregateOptions<K, V> aggregateOptions = options.apply(item);
        if (aggregateOptions == null) {
            return search.ftAggregate(indexKey, queryString, (V[]) stringOptions.apply(item).toArray());
        }
        return search.ftAggregate(indexKey, queryString, aggregateOptions);
    }

    public Function<T, AggregateOptions<K, V>> getOptions() {
        return options;
    }

    public void setOptions(Function<T, AggregateOptions<K, V>> options) {
        this.options = options;
    }

    public Function<T, List<V>> getStringOptions() {
        return stringOptions;
    }

    public void setStringOptions(Function<T, List<V>> stringOptions) {
        this.stringOptions = stringOptions;
    }

}
