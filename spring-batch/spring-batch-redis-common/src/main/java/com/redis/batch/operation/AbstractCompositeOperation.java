package com.redis.batch.operation;

import com.redis.batch.InitializingOperation;
import com.redis.batch.RedisOperation;
import io.lettuce.core.api.async.RedisAsyncCommands;

public abstract class AbstractCompositeOperation<K, V, I, O> implements InitializingOperation<K, V, I, O> {

    protected final RedisOperation<K, V, I, O> delegate;

    protected AbstractCompositeOperation(RedisOperation<K, V, I, O> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void initialize(RedisAsyncCommands<K, V> commands) throws Exception {
        if (delegate instanceof InitializingOperation) {
            ((InitializingOperation<K, V, I, O>) delegate).initialize(commands);
        }
    }

}
