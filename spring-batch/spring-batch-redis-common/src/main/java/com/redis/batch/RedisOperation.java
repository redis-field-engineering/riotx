package com.redis.batch;

import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.concurrent.Future;

public interface RedisOperation<K, V, I, O> {

    Future<O> execute(RedisAsyncCommands<K, V> commands, I item);

}
