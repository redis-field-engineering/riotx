package com.redis.batch.operation;

import com.redis.batch.RedisBatchOperation;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClassifierOperation<K, V, T, C> implements RedisBatchOperation<K, V, T, Object> {

    private RedisBatchOperation<K, V, T, Object> defaultOperation = new Noop<>();

    private final Map<C, RedisBatchOperation<K, V, T, Object>> operations = new LinkedHashMap<>();

    private final Function<T, C> classifier;

    public ClassifierOperation(Function<T, C> classifier) {
        this.classifier = classifier;
    }

    public void setOperation(C key, RedisBatchOperation<K, V, T, Object> operation) {
        operations.put(key, operation);
    }

    public void setDefaultOperation(RedisBatchOperation<K, V, T, Object> operation) {
        this.defaultOperation = operation;
    }

    @Override
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        List<Future<Object>> futures = new ArrayList<>();
        Map<C, List<T>> groupedItems = items.stream().collect(Collectors.groupingBy(classifier));
        groupedItems.forEach((c, l) -> futures.addAll(operations.getOrDefault(c, defaultOperation).execute(commands, l)));
        return futures;
    }

}
