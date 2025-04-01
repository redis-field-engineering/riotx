package com.redis.spring.batch.item.redis.reader;

import org.springframework.batch.item.ExecutionContext;

import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisPubSubHandler<K, V> extends AbstractPubSubHandler<K, V> implements RedisPubSubListener<K, V> {

	private final RedisClient client;

	private StatefulRedisPubSubConnection<K, V> connection;

	public RedisPubSubHandler(RedisClient client, RedisCodec<K, V> codec, K pattern) {
		super(codec, pattern);
		this.client = client;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (connection == null) {
			log.info("Establishing pub/sub connection to Redis");
			connection = client.connectPubSub(codec);
			connection.addListener(this);
			log.info(String.format("Subscribing to pattern %s", BatchUtils.toString(pattern)));
			connection.sync().psubscribe(pattern);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void close() {
		if (connection != null) {
			log.info(String.format("Unsubscribing from pattern %s", BatchUtils.toString(pattern)));
			connection.sync().punsubscribe(pattern);
			connection.removeListener(this);
			log.info("Closing pub/sub connection");
			connection.close();
			connection = null;
		}
	}

	@Override
	public void message(K pattern, K channel, V message) {
		notifyConsumers(channel, message);
	}

	@Override
	public void psubscribed(K pattern, long count) {
		log.info(String.format("Subscribed to pattern %s", BatchUtils.toString(pattern)));
	}

	@Override
	public void punsubscribed(K pattern, long count) {
		log.info(String.format("Unsubscribed from pattern %s", BatchUtils.toString(pattern)));
	}

	@Override
	public void message(K channel, V message) {
		// ignore
	}

	@Override
	public void subscribed(K channel, long count) {
		// ignore
	}

	@Override
	public void unsubscribed(K channel, long count) {
		// ignore
	}

}