package com.redis.batch.operation;

import com.redis.batch.KeyStructEvent;
import com.redis.batch.KeyType;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;

import java.util.*;
import java.util.stream.Collectors;

public class KeyStructRead<K, V> extends AbstractKeyValueRead<K, V, KeyStructEvent<K, V>> {

    public KeyStructRead(RedisCodec<K, V> codec) {
        super(codec, Mode.struct);
    }

    @Override
    protected void setValue(KeyStructEvent<K, V> keyValue, Object value) {
        keyValue.setValue(value(keyValue.getType(), value));
    }

    @SuppressWarnings("unchecked")
    private Object value(KeyType type, Object value) {
        switch (type) {
            case hash:
                return map((List<Object>) value);
            case set:
                return new HashSet<>((Collection<V>) value);
            case stream:
                return streamMessages((Collection<List<Object>>) value);
            case timeseries:
                return timeseries((List<List<Object>>) value);
            case zset:
                return zset(value);
            default:
                return value;
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

    @Override
    protected KeyStructEvent<K, V> keyValueEvent() {
        return new KeyStructEvent<>();
    }

}
