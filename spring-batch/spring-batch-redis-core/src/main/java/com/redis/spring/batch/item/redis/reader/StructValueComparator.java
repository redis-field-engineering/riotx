package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyType;
import com.redis.batch.KeyValueEvent;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import org.springframework.util.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StructValueComparator<K, V> {

    private final Function<Object, Object> valueTransformer;

    private final Map<KeyType, ValueComparisonStrategy> comparisonStrategies;

    private boolean ignoreStreamMessageIds;

    public StructValueComparator(RedisCodec<K, V> codec) {
        this.valueTransformer = createValueTransformer(codec);
        this.comparisonStrategies = initializeComparisonStrategies();
    }

    public boolean compare(KeyValueEvent<K> source, KeyValueEvent<K> target) {
        // Handle reference equality and null cases
        if (source == target)
            return true;
        if (source == null || target == null)
            return false;

        // Extract and compare values
        Object sourceValue = source.getValue();
        Object targetValue = target.getValue();
        if (sourceValue == targetValue)
            return true;
        if (sourceValue == null || targetValue == null)
            return false;

        // Determine comparison strategy based on key type
        KeyType keyType = KeyType.of(source.getType());
        return keyType != null
                ? compareByType(keyType, sourceValue, targetValue)
                : Objects.deepEquals(sourceValue, targetValue);
    }

    private Function<Object, Object> createValueTransformer(RedisCodec<K, V> codec) {
        return codec instanceof ByteArrayCodec
                ? value -> value != null ? ByteBuffer.wrap((byte[]) value) : null
                : Function.identity();
    }

    private Map<KeyType, ValueComparisonStrategy> initializeComparisonStrategies() {
        Map<KeyType, ValueComparisonStrategy> strategies = new EnumMap<>(KeyType.class);

        strategies.put(KeyType.STRING, this::compareValues);
        strategies.put(KeyType.JSON, this::compareValues);
        strategies.put(KeyType.HASH, this::compareMaps);
        strategies.put(KeyType.LIST, this::compareLists);
        strategies.put(KeyType.SET, this::compareSets);
        strategies.put(KeyType.ZSET, this::compareZSets);
        strategies.put(KeyType.STREAM, this::compareStreams);

        return Collections.unmodifiableMap(strategies);
    }

    private boolean compareByType(KeyType keyType, Object source, Object target) {
        ValueComparisonStrategy strategy = comparisonStrategies.get(keyType);
        return strategy != null ? strategy.compare(source, target) : Objects.deepEquals(source, target);
    }

    @SuppressWarnings("unchecked")
    private boolean compareValues(Object source, Object target) {
        try {
            Object transformedSource = valueTransformer.apply((V) source);
            Object transformedTarget = valueTransformer.apply((V) target);
            return Objects.equals(transformedSource, transformedTarget);
        } catch (ClassCastException e) {
            // Fallback to deep equals if casting fails
            return Objects.deepEquals(source, target);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean compareMaps(Object source, Object target) {
        try {
            Map<K, V> sourceMap = (Map<K, V>) source;
            Map<K, V> targetMap = (Map<K, V>) target;

            if (sourceMap.size() != targetMap.size())
                return false;
            if (sourceMap.isEmpty())
                return true; // Both are empty

            Map<Object, Object> transformedSource = transformMap(sourceMap);
            Map<Object, Object> transformedTarget = transformMap(targetMap);

            return Objects.equals(transformedSource, transformedTarget);
        } catch (ClassCastException e) {
            return Objects.deepEquals(source, target);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean compareLists(Object source, Object target) {
        try {
            Collection<V> sourceList = (Collection<V>) source;
            Collection<V> targetList = (Collection<V>) target;

            if (sourceList.size() != targetList.size())
                return false;
            if (sourceList.isEmpty())
                return true; // Both are empty

            List<Object> transformedSource = transformCollection(sourceList);
            List<Object> transformedTarget = transformCollection(targetList);

            return Objects.equals(transformedSource, transformedTarget);
        } catch (ClassCastException e) {
            return Objects.deepEquals(source, target);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean compareSets(Object source, Object target) {
        try {
            Collection<V> sourceSet = (Collection<V>) source;
            Collection<V> targetSet = (Collection<V>) target;

            if (sourceSet.size() != targetSet.size())
                return false;
            if (sourceSet.isEmpty())
                return true; // Both are empty

            Set<Object> transformedSource = transformToSet(sourceSet);
            Set<Object> transformedTarget = transformToSet(targetSet);

            return Objects.equals(transformedSource, transformedTarget);
        } catch (ClassCastException e) {
            return Objects.deepEquals(source, target);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean compareZSets(Object source, Object target) {
        try {
            Collection<ScoredValue<V>> sourceZSet = (Collection<ScoredValue<V>>) source;
            Collection<ScoredValue<V>> targetZSet = (Collection<ScoredValue<V>>) target;

            if (sourceZSet.size() != targetZSet.size())
                return false;
            if (sourceZSet.isEmpty())
                return true; // Both are empty

            Set<ScoredValue<Object>> transformedSource = transformZSet(sourceZSet);
            Set<ScoredValue<Object>> transformedTarget = transformZSet(targetZSet);

            return Objects.equals(transformedSource, transformedTarget);
        } catch (ClassCastException e) {
            return Objects.deepEquals(source, target);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean compareStreams(Object source, Object target) {
        try {
            Collection<StreamMessage<K, V>> sourceStream = (Collection<StreamMessage<K, V>>) source;
            Collection<StreamMessage<K, V>> targetStream = (Collection<StreamMessage<K, V>>) target;

            if (CollectionUtils.isEmpty(sourceStream)) {
                return CollectionUtils.isEmpty(targetStream);
            }

            if (sourceStream.size() != targetStream.size()) {
                return false;
            }

            return compareStreamMessages(sourceStream, targetStream);
        } catch (ClassCastException e) {
            return Objects.deepEquals(source, target);
        }
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
            if (!compareMaps(sourceMsg.getBody(), targetMsg.getBody())) {
                return false;
            }
        }

        // Ensure both iterators are exhausted
        return !sourceIter.hasNext() && !targetIter.hasNext();
    }

    private Map<Object, Object> transformMap(Map<K, V> map) {
        if (map.isEmpty())
            return Collections.emptyMap();

        return map.entrySet().stream().collect(Collectors.toMap(entry -> valueTransformer.apply(entry.getKey()),
                entry -> valueTransformer.apply(entry.getValue()), (existing, replacement) -> existing,
                // Handle duplicate keys consistently
                HashMap::new));
    }

    private List<Object> transformCollection(Collection<V> collection) {
        if (collection.isEmpty())
            return Collections.emptyList();

        return collection.stream().map(valueTransformer).collect(Collectors.toList());
    }

    private Set<Object> transformToSet(Collection<V> collection) {
        if (collection.isEmpty())
            return Collections.emptySet();

        return collection.stream().map(valueTransformer).collect(Collectors.toSet());
    }

    private Set<ScoredValue<Object>> transformZSet(Collection<ScoredValue<V>> zset) {
        if (zset.isEmpty())
            return Collections.emptySet();

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

    @FunctionalInterface
    public interface ValueComparisonStrategy {

        /**
         * Compare two objects of potentially different Redis data types
         *
         * @param source the source object
         * @param target the target object
         * @return true if the objects are considered equal, false otherwise
         */
        boolean compare(Object source, Object target);

    }

}
