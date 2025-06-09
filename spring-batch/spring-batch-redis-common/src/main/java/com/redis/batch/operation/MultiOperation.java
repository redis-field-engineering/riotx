package com.redis.batch.operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.redis.batch.InitializingOperation;
import com.redis.batch.RedisOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class MultiOperation<K, V, I, O> implements InitializingOperation<K, V, I, O> {

    protected final Iterable<RedisOperation<K, V, I, O>> delegates;

    @SuppressWarnings("unchecked")
    public MultiOperation(RedisOperation<K, V, I, O>... delegates) {
        this(Arrays.asList(delegates));
    }

    public MultiOperation(Iterable<RedisOperation<K, V, I, O>> delegates) {
        this.delegates = delegates;
    }

    @Override
    public void initialize(RedisAsyncCommands<K, V> commands) throws Exception {
        for (RedisOperation<K, V, I, O> delegate : delegates) {
            if (delegate instanceof InitializingOperation) {
                ((InitializingOperation<K, V, I, O>) delegate).initialize(commands);
            }
        }
    }

    @Override
    public List<RedisFuture<O>> execute(RedisAsyncCommands<K, V> commands, List<? extends I> items) {
        List<RedisFuture<O>> futures = new ArrayList<>();
        for (RedisOperation<K, V, I, O> delegate : delegates) {
            futures.addAll(delegate.execute(commands, items));
        }
        return futures;
    }

}
