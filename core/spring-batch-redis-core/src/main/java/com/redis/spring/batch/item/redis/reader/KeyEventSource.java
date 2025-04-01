package com.redis.spring.batch.item.redis.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.RedisCodec;

public class KeyEventSource<K, V> implements ItemStream {

	private static final String KEYSPACE_PATTERN = "__keyspace@%s__:%s";
	private static final String KEYEVENT_PATTERN = "__keyevent@%s__:*";
	private static final String SEPARATOR = ":";

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final Function<String, K> keyEncoder;
	private final Function<K, String> keyDecoder;
	private final Function<V, String> valueDecoder;
	private final List<KeyEventListener<K>> listeners = new ArrayList<>();

	private int database;
	private String keyPattern;
	private String keyType;

	private PubSubHandler<K, V> publisher;

	public KeyEventSource(AbstractRedisClient client, RedisCodec<K, V> codec) {
		this.client = client;
		this.codec = codec;
		this.keyEncoder = BatchUtils.stringKeyFunction(codec);
		this.keyDecoder = BatchUtils.toStringKeyFunction(codec);
		this.valueDecoder = BatchUtils.toStringValueFunction(codec);
	}

	public void addListener(KeyEventListener<K> listener) {
		listeners.add(listener);
	}

	public String pubSubPattern() {
		if (isKeyEvents()) {
			return String.format(KEYEVENT_PATTERN, database);
		}
		return String.format(KEYSPACE_PATTERN, database, keyPattern);
	}

	private boolean isKeyEvents() {
		return keyPattern == null;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		if (publisher == null) {
			publisher = publisher();
			publisher.addConsumer(messageConsumer());
			publisher.open(executionContext);
		}
	}

	public boolean isOpen() {
		return publisher != null;
	}

	@Override
	public void update(ExecutionContext executionContext) {
		publisher.update(executionContext);
	}

	@Override
	public void close() {
		if (publisher != null) {
			publisher.close();
			publisher = null;
		}
	}

	private void keySpaceNotification(K channel, V message) {
		K key = keyEncoder.apply(suffix(channel));
		String event = valueDecoder.apply(message);
		keyEvent(key, event);
	}

	@SuppressWarnings("unchecked")
	private void keyEventNotification(K channel, V message) {
		K key = (K) message;
		String event = suffix(channel);
		keyEvent(key, event);
	}

	private void keyEvent(K key, String event) {
		String type = type(event);
		if (keyType == null || keyType.equalsIgnoreCase(type)) {
			KeyEvent<K> keyEvent = new KeyEvent<>();
			keyEvent.setKey(key);
			keyEvent.setEvent(event);
			keyEvent.setTimestamp(System.currentTimeMillis());
			keyEvent.setType(type);
			listeners.forEach(c -> c.keyEvent(keyEvent));
		}
	}

	public static String type(String event) {
		if (event == null) {
			return null;
		}
		if (event.startsWith("xgroup-")) {
			return KeyValue.TYPE_STREAM;
		}
		if (event.startsWith("ts.")) {
			return KeyValue.TYPE_TIMESERIES;
		}
		if (event.startsWith("json.")) {
			return KeyValue.TYPE_JSON;
		}
		switch (event) {
		case "set":
		case "setrange":
		case "incrby":
		case "incrbyfloat":
		case "append":
			return KeyValue.TYPE_STRING;
		case "lpush":
		case "rpush":
		case "rpop":
		case "lpop":
		case "linsert":
		case "lset":
		case "lrem":
		case "ltrim":
			return KeyValue.TYPE_LIST;
		case "hset":
		case "hincrby":
		case "hincrbyfloat":
		case "hdel":
			return KeyValue.TYPE_HASH;
		case "sadd":
		case "spop":
		case "sinterstore":
		case "sunionstore":
		case "sdiffstore":
			return KeyValue.TYPE_SET;
		case "zincr":
		case "zadd":
		case "zrem":
		case "zrembyscore":
		case "zrembyrank":
		case "zdiffstore":
		case "zinterstore":
		case "zunionstore":
			return KeyValue.TYPE_ZSET;
		case "xadd":
		case "xtrim":
		case "xdel":
		case "xsetid":
			return KeyValue.TYPE_STREAM;
		default:
			return null;
		}
	}

	private BiConsumer<K, V> messageConsumer() {
		if (isKeyEvents()) {
			return this::keyEventNotification;
		}
		return this::keySpaceNotification;
	}

	private String suffix(K key) {
		String string = keyDecoder.apply(key);
		int index = string.indexOf(SEPARATOR);
		if (index > 0) {
			return string.substring(index + 1);
		}
		return null;
	}

	private PubSubHandler<K, V> publisher() {
		String pubSubPattern = pubSubPattern();
		K pattern = keyEncoder.apply(pubSubPattern);
		if (client instanceof RedisClusterClient) {
			return new RedisClusterPubSubHandler<>((RedisClusterClient) client, codec, pattern);
		}
		return new RedisPubSubHandler<>((RedisClient) client, codec, pattern);
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public String getKeyPattern() {
		return keyPattern;
	}

	public void setKeyPattern(String pattern) {
		this.keyPattern = pattern;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String type) {
		this.keyType = type;
	}

}