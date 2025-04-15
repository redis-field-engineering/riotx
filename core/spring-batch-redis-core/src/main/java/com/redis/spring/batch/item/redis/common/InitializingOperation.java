package com.redis.spring.batch.item.redis.common;

import io.lettuce.core.api.async.RedisAsyncCommands;

public interface InitializingOperation<K, V, I, O> extends RedisOperation<K, V, I, O> {

    void initialize(RedisAsyncCommands<K, V> commands) throws Exception;

}
