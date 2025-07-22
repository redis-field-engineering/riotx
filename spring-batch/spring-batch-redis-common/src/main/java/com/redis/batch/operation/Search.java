package com.redis.batch.operation;

import com.redis.batch.RedisBatchOperation;
import com.redis.lettucemod.api.async.RediSearchAsyncCommands;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchResults;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

public class Search<K, V, T> implements RedisBatchOperation<K, V, T, SearchResults<K, V>> {

    private final Function<T, K> index;

    private final Function<T, V> query;

    private Function<T, SearchOptions<K, V>> options = t -> null;

    private Function<T, List<V>> stringOptions = t -> Collections.emptyList();

    public Search(Function<T, K> index, Function<T, V> query) {
        this.index = index;
        this.query = query;
    }

    @Override
    public List<? extends Future<SearchResults<K, V>>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return execute(commands, items, this::execute);
    }

    @SuppressWarnings("unchecked")
    private RedisFuture<SearchResults<K, V>> execute(RedisAsyncCommands<K, V> commands, T item) {
        RediSearchAsyncCommands<K, V> search = (RediSearchAsyncCommands<K, V>) commands;
        K indexKey = index.apply(item);
        V queryString = query.apply(item);
        SearchOptions<K, V> searchOptions = options.apply(item);
        if (searchOptions == null) {
            return search.ftSearch(indexKey, queryString, (V[]) stringOptions.apply(item).toArray());
        }
        return search.ftSearch(indexKey, queryString, searchOptions);
    }

    public Function<T, SearchOptions<K, V>> getOptions() {
        return options;
    }

    public void setOptions(Function<T, SearchOptions<K, V>> options) {
        this.options = options;
    }

    public Function<T, List<V>> getStringOptions() {
        return stringOptions;
    }

    public void setStringOptions(Function<T, List<V>> stringOptions) {
        this.stringOptions = stringOptions;
    }

}
