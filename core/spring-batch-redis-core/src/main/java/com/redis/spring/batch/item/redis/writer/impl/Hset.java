package com.redis.spring.batch.item.redis.writer.impl;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.batch.item.Chunk;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Hset<K, V, T> extends AbstractValueWriteOperation<K, V, Map<K, V>, T> {

	public Hset(Function<T, K> keyFunction, Function<T, Map<K, V>> valueFunction) {
		super(keyFunction, valueFunction);
	}

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Chunk<? extends T> items) {
		return BatchUtils.executeAll(commands, items, this::execute);
	}

	private RedisFuture<Long> execute(RedisAsyncCommands<K, V> commands, T item) {
		Map<K, V> value = value(item);
		if (CollectionUtils.isEmpty(value)) {
			return null;
		}
		return commands.hset(key(item), value);
	}

}
