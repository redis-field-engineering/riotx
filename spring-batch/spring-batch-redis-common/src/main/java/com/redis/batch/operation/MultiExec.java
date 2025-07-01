package com.redis.batch.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.redis.batch.RedisBatchOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class MultiExec<K, V, T> extends AbstractCompositeWrite<K, V, T> {

    public MultiExec(RedisBatchOperation<K, V, T, Object> delegate) {
        super(delegate);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        List<Future<Object>> futures = new ArrayList<>();
        futures.add((RedisFuture) commands.multi());
        futures.addAll(delegate.execute(commands, items));
        futures.add((RedisFuture) commands.exec());
        return futures;
    }

}
