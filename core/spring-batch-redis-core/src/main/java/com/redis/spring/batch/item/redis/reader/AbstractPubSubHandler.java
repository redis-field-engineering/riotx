package com.redis.spring.batch.item.redis.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractPubSubHandler<K, V> implements PubSubHandler<K, V> {

	protected final Log log = LogFactory.getLog(getClass());
	protected final RedisCodec<K, V> codec;
	protected final K pattern;
	private final List<BiConsumer<K, V>> consumers = new ArrayList<>();

	protected AbstractPubSubHandler(RedisCodec<K, V> codec, K pattern) {
		this.codec = codec;
		this.pattern = pattern;
	}

	@Override
	public void addConsumer(BiConsumer<K, V> consumer) {
		this.consumers.add(consumer);
	}

	protected void notifyConsumers(K channel, V message) {
		consumers.forEach(l -> l.accept(channel, message));
	}

}
