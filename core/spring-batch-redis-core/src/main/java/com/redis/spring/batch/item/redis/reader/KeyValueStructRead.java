package com.redis.spring.batch.item.redis.reader;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;

public class KeyValueStructRead<K, V> extends KeyValueRead<K, V, Object> {

	public KeyValueStructRead(RedisCodec<K, V> codec) {
		super(ValueType.STRUCT, codec);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object value(KeyValue<K> item, Object value) {
		if (value == null || item.getType() == null) {
			return value;
		}
		switch (item.getType()) {
		case KeyValue.TYPE_HASH:
			return map((List<Object>) value);
		case KeyValue.TYPE_SET:
			return new HashSet<>((Collection<V>) value);
		case KeyValue.TYPE_STREAM:
			return streamMessages(item.getKey(), (Collection<List<Object>>) value);
		case KeyValue.TYPE_TIMESERIES:
			return timeseries((List<List<Object>>) value);
		case KeyValue.TYPE_ZSET:
			return zset(value);
		default:
			return value;
		}
	}

	private List<Sample> timeseries(List<List<Object>> value) {
		return value.stream().map(this::sample).collect(Collectors.toList());
	}

	private List<StreamMessage<K, V>> streamMessages(K key, Collection<List<Object>> value) {
		return value.stream().map(v -> message(key, v)).collect(Collectors.toList());
	}

	private Sample sample(List<Object> sample) {
		LettuceAssert.isTrue(sample.size() == 2, "Invalid list size: " + sample.size());
		Long timestamp = (Long) sample.get(0);
		return Sample.of(timestamp, toDouble(sample.get(1)));
	}

	private double toDouble(Object value) {
		return Double.parseDouble(toString(value));
	}

	@SuppressWarnings("unchecked")
	private Map<K, V> map(List<Object> list) {
		LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
		Map<K, V> map = new HashMap<>();
		for (int i = 0; i < list.size(); i += 2) {
			map.put((K) list.get(i), (V) list.get(i + 1));
		}
		return map;
	}

	@SuppressWarnings("unchecked")
	private Set<ScoredValue<V>> zset(Object value) {
		List<Object> list = (List<Object>) value;
		LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
		Set<ScoredValue<V>> values = new HashSet<>();
		for (int i = 0; i < list.size(); i += 2) {
			double score = toDouble(list.get(i + 1));
			values.add(ScoredValue.just(score, (V) list.get(i)));
		}
		return values;
	}

	@SuppressWarnings("unchecked")
	private StreamMessage<K, V> message(K key, List<Object> message) {
		LettuceAssert.isTrue(message.size() == 2, "Invalid list size: " + message.size());
		String id = toString(message.get(0));
		Map<K, V> body = map((List<Object>) message.get(1));
		return new StreamMessage<>(key, id, body);

	}

}
