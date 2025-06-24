package com.redis.batch.operation;

import java.util.Collections;
import java.util.List;

import com.redis.batch.RedisBatchOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Noop<K, V, T> implements RedisBatchOperation<K, V, T, Object> {

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return Collections.emptyList();
    }

}
