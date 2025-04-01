package com.redis.spring.batch.item.redis.reader;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.RedisCodec;

public class RedisClusterPubSubHandler<K, V> extends AbstractPubSubHandler<K, V>
		implements RedisClusterPubSubListener<K, V> {

	private final RedisClusterClient client;

	private StatefulRedisClusterPubSubConnection<K, V> connection;

	public RedisClusterPubSubHandler(RedisClusterClient client, RedisCodec<K, V> codec, K pattern) {
		super(codec, pattern);
		this.client = client;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		if (connection == null) {
			log.info("Establishing pub/sub connection to Redis cluster");
			connection = client.connectPubSub(codec);
			log.info("Enabling node message propagation");
			connection.setNodeMessagePropagation(true);
			connection.addListener(this);
			log.info(String.format("Subscribing to pattern %s", BatchUtils.toString(pattern)));
			connection.sync().upstream().commands().psubscribe(pattern);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void close() {
		if (connection != null) {
			log.info(String.format("Unsubscribing from pattern %s", BatchUtils.toString(pattern)));
			connection.sync().upstream().commands().punsubscribe(pattern);
			connection.removeListener(this);
			log.info("Closing pub/sub connection");
			connection.close();
			connection = null;
		}
	}

	@Override
	public void message(RedisClusterNode node, K pattern, K channel, V message) {
		notifyConsumers(channel, message);
	}

	@Override
	public void psubscribed(RedisClusterNode node, K pattern, long count) {
		log.info(String.format("Subscribed to pattern %s", BatchUtils.toString(pattern)));
	}

	@Override
	public void punsubscribed(RedisClusterNode node, K pattern, long count) {
		log.info(String.format("Unsubscribed from pattern %s", BatchUtils.toString(pattern)));
	}

	@Override
	public void message(RedisClusterNode node, K channel, V message) {
		// ignore
	}

	@Override
	public void subscribed(RedisClusterNode node, K channel, long count) {
		// ignore
	}

	@Override
	public void unsubscribed(RedisClusterNode node, K channel, long count) {
		// ignore
	}

}