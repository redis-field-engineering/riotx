package com.redis.batch.operation;

import java.util.Collection;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Rpush<K, V, T> extends AbstractMemberWrite<K, V, T> {

	public Rpush(Function<T, K> keyFunction, Function<T, Collection<V>> valueFunction) {
		super(keyFunction, valueFunction);
	}

	@Override
	protected RedisFuture<Long> execute(RedisAsyncCommands<K, V> commands, K key, V[] values) {
		return commands.rpush(key, values);
	}

}
