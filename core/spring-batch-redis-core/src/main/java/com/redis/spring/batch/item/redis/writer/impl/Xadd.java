package com.redis.spring.batch.item.redis.writer.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.batch.item.Chunk;
import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Xadd<K, V, T> extends AbstractValueWriteOperation<K, V, Collection<StreamMessage<K, V>>, T> {

	private Function<StreamMessage<K, V>, XAddArgs> argsFunction = this::defaultArgs;

	public Xadd(Function<T, K> keyFunction, Function<T, Collection<StreamMessage<K, V>>> messagesFunction) {
		super(keyFunction, messagesFunction);
	}

	private XAddArgs defaultArgs(StreamMessage<K, V> message) {
		if (message == null || message.getId() == null) {
			return null;
		}
		return new XAddArgs().id(message.getId());
	}

	public void setArgs(XAddArgs args) {
		setArgsFunction(t -> args);
	}

	public void setArgsFunction(Function<StreamMessage<K, V>, XAddArgs> function) {
		this.argsFunction = function;
	}

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Chunk<? extends T> items) {
		List<RedisFuture<Object>> futures = new ArrayList<>();
		for (T item : items) {
			futures.addAll(execute(commands, item));
		}
		return futures;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, T item) {
		K key = key(item);
		Collection<StreamMessage<K, V>> messages = value(item);
		if (CollectionUtils.isEmpty(messages)) {
			Map<K, V> dummyBody = new HashMap<>();
			dummyBody.put(key, (V) key);
			return (List) Arrays.asList(commands.xadd(key, dummyBody), commands.xtrim(key, 0));
		}
		return messages.stream().filter(this::hasBody).map(m -> execute(commands, key, m)).collect(Collectors.toList());
	}

	private boolean hasBody(StreamMessage<K, V> message) {
		return !CollectionUtils.isEmpty(message.getBody());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RedisFuture<Object> execute(RedisAsyncCommands<K, V> commands, K key, StreamMessage<K, V> message) {
		Map<K, V> body = message.getBody();
		XAddArgs args = argsFunction.apply(message);
		return (RedisFuture) commands.xadd(key, args, body);
	}

}
