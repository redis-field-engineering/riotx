package com.redis.batch;

import io.lettuce.core.api.async.RedisAsyncCommands;

import java.time.Duration;

public interface InitializingOperation<K, V, I, O> extends RedisBatchOperation<K, V, I, O> {

    void initialize(RedisAsyncCommands<K, V> commands) throws Exception;

    @SuppressWarnings("deprecation")
    default Duration timeout(RedisAsyncCommands<K, V> commands) {
        return commands.getStatefulConnection().getTimeout();
    }

}
