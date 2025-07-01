package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyStructEvent;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StructValueComparator<K, V> {

    private final Function<Object, Object> valueTransformer;

    private boolean ignoreStreamMessageIds;

    public StructValueComparator(RedisCodec<K, V> codec) {
        this.valueTransformer = createValueTransformer(codec);
    }

    private Function<Object, Object> createValueTransformer(RedisCodec<K, V> codec) {
        if (codec instanceof ByteArrayCodec) {
            return this::wrapBytes;
        }
        return Function.identity();
    }

    private Object wrapBytes(Object value) {
        if (value == null) {
            return null;
        }
        return ByteBuffer.wrap((byte[]) value);
    }

    public boolean compare(KeyStructEvent<K, V> source, KeyStructEvent<K, V> target) {
        // Handle reference equality and null cases
        if (source == target) {
            return true;
        }
        if (source == null || target == null) {
            return false;
        }
        if (source.getValue() == target.getValue()) {
            return true;
        }
        if (source.getValue() == null || target.getValue() == null) {
            return false;
        }
        // Determine comparison strategy based on key type
        switch (source.getType()) {
            case hash:
                return compareHash(source.asHash(), target.asHash());
            case json:
                return compareString(source.asJson(), target.asJson());
            case list:
                return compareCollection(source.asList(), target.asList());
            case set:
                return compareSet(source.asSet(), target.asSet());
            case stream:
                return compareStream(source.asStream(), target.asStream());
            case string:
                return compareString(source.asString(), target.asString());
            case timeseries:
                return compareTimeseries(source.asTimeseries(), target.asTimeseries());
            case zset:
                return compareZset(source.asZSet(), target.asZSet());
            default:
                return Objects.deepEquals(source.getValue(), target.getValue());
        }
    }

    private boolean compareString(V source, V target) {
        return Objects.equals(value(source), value(target));
    }

    private boolean compareHash(Map<K, V> sourceMap, Map<K, V> targetMap) {
        return Objects.deepEquals(transformMap(sourceMap), transformMap(targetMap));
    }

    private boolean compareSet(Set<V> source, Set<V> target) {
        return Objects.deepEquals(new HashSet<>(transformCollection(source)), new HashSet<>(transformCollection(target)));
    }

    private boolean compareCollection(Collection<V> source, Collection<V> target) {
        return Objects.deepEquals(transformCollection(source), transformCollection(target));
    }

    private boolean compareZset(Collection<ScoredValue<V>> source, Collection<ScoredValue<V>> target) {
        return Objects.deepEquals(transformZSet(source), transformZSet(target));
    }

    private boolean compareTimeseries(Collection<Sample> source, Collection<Sample> target) {
        return Objects.deepEquals(source, target);
    }

    private boolean compareStream(Collection<StreamMessage<K, V>> source, Collection<StreamMessage<K, V>> target) {
        if (source.size() != target.size()) {
            return false;
        }
        return compareStreamMessages(source, target);
    }

    private boolean compareStreamMessages(Collection<StreamMessage<K, V>> source, Collection<StreamMessage<K, V>> target) {
        Iterator<StreamMessage<K, V>> sourceIter = source.iterator();
        Iterator<StreamMessage<K, V>> targetIter = target.iterator();

        while (sourceIter.hasNext() && targetIter.hasNext()) {
            StreamMessage<K, V> sourceMsg = sourceIter.next();
            StreamMessage<K, V> targetMsg = targetIter.next();

            // Check message IDs if policy requires it
            if (!ignoreStreamMessageIds && !Objects.equals(sourceMsg.getId(), targetMsg.getId())) {
                return false;
            }

            // Compare message bodies
            if (!compareHash(sourceMsg.getBody(), targetMsg.getBody())) {
                return false;
            }
        }

        // Ensure both iterators are exhausted
        return !sourceIter.hasNext() && !targetIter.hasNext();
    }

    private Map<Object, Object> transformMap(Map<K, V> map) {
        return map.entrySet().stream().collect(
                Collectors.toMap(entry -> value(entry.getKey()), entry -> value(entry.getValue()),
                        (existing, replacement) -> existing,
                        // Handle duplicate keys consistently
                        HashMap::new));
    }

    private Object value(Object value) {
        return valueTransformer.apply(value);
    }

    private List<Object> transformCollection(Collection<V> collection) {
        return collection.stream().map(valueTransformer).collect(Collectors.toList());
    }

    private Set<Object> transformToSet(Collection<V> collection) {
        return collection.stream().map(valueTransformer).collect(Collectors.toSet());
    }

    private Set<ScoredValue<Object>> transformZSet(Collection<ScoredValue<V>> zset) {
        return zset.stream()
                .map(scoredValue -> ScoredValue.just(scoredValue.getScore(), valueTransformer.apply(scoredValue.getValue())))
                .collect(Collectors.toSet());
    }

    public boolean isIgnoreStreamMessageIds() {
        return ignoreStreamMessageIds;
    }

    public void setIgnoreStreamMessageIds(boolean ignoreStreamMessageIds) {
        this.ignoreStreamMessageIds = ignoreStreamMessageIds;
    }

}
