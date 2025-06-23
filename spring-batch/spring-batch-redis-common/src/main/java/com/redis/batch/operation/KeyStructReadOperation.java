package com.redis.batch.operation;

import com.redis.batch.KeyType;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;

import java.util.*;
import java.util.stream.Collectors;

public class KeyStructReadOperation<K, V> extends KeyValueReadOperation<K, V> {

    public KeyStructReadOperation(RedisCodec<K, V> codec) {
        super(codec, Mode.STRUCT);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object value(ReadResult<V> result) {
        KeyType type = KeyType.of(toString(result.getType()));
        if (type == null) {
            return null;
        }
        switch (type) {
            case HASH:
                return map((List<Object>) result.getValue());
            case SET:
                return new HashSet<>((Collection<V>) result.getValue());
            case STREAM:
                return streamMessages((Collection<List<Object>>) result.getValue());
            case TIMESERIES:
                return timeseries((List<List<Object>>) result.getValue());
            case ZSET:
                return zset(result.getValue());
            default:
                return result.getValue();
        }
    }

    private List<StreamMessage<K, V>> streamMessages(Collection<List<Object>> value) {
        return value.stream().map(this::message).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private Sample sample(List<Object> sample) {
        LettuceAssert.isTrue(sample.size() == 2, "Invalid list size: " + sample.size());
        Long timestamp = (Long) sample.get(0);
        return Sample.of(timestamp, toDouble((V) sample.get(1)));
    }

    private double toDouble(V value) {
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
    private java.util.Set<ScoredValue<V>> zset(Object value) {
        List<V> list = (List<V>) value;
        LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
        java.util.Set<ScoredValue<V>> values = new HashSet<>();
        for (int i = 0; i < list.size(); i += 2) {
            double score = toDouble(list.get(i + 1));
            values.add(ScoredValue.just(score, list.get(i)));
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    private StreamMessage<K, V> message(List<Object> message) {
        LettuceAssert.isTrue(message.size() == 2, "Invalid list size: " + message.size());
        String id = toString((V) message.get(0));
        Map<K, V> body = map((List<Object>) message.get(1));
        return new StreamMessage<>(null, id, body);
    }

    private List<Sample> timeseries(List<List<Object>> value) {
        return value.stream().map(this::sample).collect(Collectors.toList());
    }

}
