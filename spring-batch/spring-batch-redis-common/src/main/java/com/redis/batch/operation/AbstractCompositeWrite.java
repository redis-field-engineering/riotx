package com.redis.batch.operation;

import com.redis.batch.InitializingOperation;
import com.redis.batch.RedisBatchOperation;
import io.lettuce.core.api.async.RedisAsyncCommands;

public abstract class AbstractCompositeWrite<K, V, I> implements InitializingOperation<K, V, I, Object> {

    protected final RedisBatchOperation<K, V, I, Object> delegate;

    protected AbstractCompositeWrite(RedisBatchOperation<K, V, I, Object> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void initialize(RedisAsyncCommands<K, V> commands) throws Exception {
        if (delegate instanceof InitializingOperation) {
            ((InitializingOperation<K, V, I, Object>) delegate).initialize(commands);
        }
    }

}
