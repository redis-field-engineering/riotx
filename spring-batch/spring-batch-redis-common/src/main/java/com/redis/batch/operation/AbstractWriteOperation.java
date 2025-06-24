package com.redis.batch.operation;

import java.util.function.Function;

import com.redis.batch.RedisBatchOperation;

public abstract class AbstractWriteOperation<K, V, T> implements RedisBatchOperation<K, V, T, Object> {

    protected final Function<T, K> keyFunction;

    protected AbstractWriteOperation(Function<T, K> keyFunction) {
        this.keyFunction = keyFunction;
    }

    protected K key(T item) {
        return keyFunction.apply(item);
    }

}
