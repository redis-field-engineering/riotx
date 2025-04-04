package com.redis.spring.batch.item.redis.writer.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.RedisOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class ClassifierOperation<K, V, T, C> implements RedisOperation<K, V, T, Object> {

    private RedisOperation<K, V, T, Object> defaultOperation = new Noop<>();

    private final Map<C, RedisOperation<K, V, T, Object>> operations = new LinkedHashMap<>();

    private final Function<T, C> classifier;

    public ClassifierOperation(Function<T, C> classifier) {
        this.classifier = classifier;
    }

    public void setOperation(C key, RedisOperation<K, V, T, Object> operation) {
        operations.put(key, operation);
    }

    public void setDefaultOperation(RedisOperation<K, V, T, Object> operation) {
        this.defaultOperation = operation;
    }

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
        List<RedisFuture<Object>> futures = new ArrayList<>();
        Map<C, List<T>> groupedItems = BatchUtils.stream(items).collect(Collectors.groupingBy(classifier));
        groupedItems.forEach(
                (c, l) -> futures.addAll(operations.getOrDefault(c, defaultOperation).execute(commands, new Chunk<>(l))));
        return futures;
    }

}
