package com.redis.spring.batch.item.redis.writer.impl;

import java.util.Collections;
import java.util.List;

import com.redis.spring.batch.item.redis.common.RedisOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Noop<K, V, T> implements RedisOperation<K, V, T, Object> {

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
        return Collections.emptyList();
    }

}
