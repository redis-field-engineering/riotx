package com.redis.spring.batch.item.redis.reader;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.redis.batch.KeyEvent;
import org.springframework.util.CollectionUtils;

import com.redis.batch.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;

public class DefaultKeyComparator<K, V> implements KeyComparator<K> {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    public static final boolean DEFAULT_STREAM_MESSAGE_IDS = true;

    private final Function<Object, Object> hashFunction;

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    private boolean streamMessageIds = DEFAULT_STREAM_MESSAGE_IDS;

    public DefaultKeyComparator(RedisCodec<K, V> codec) {
        this.hashFunction = codec instanceof ByteArrayCodec ? t -> ByteBuffer.wrap((byte[]) t) : t -> t;
    }

    @Override
    public KeyComparison<K> compare(KeyValue<K> source, KeyValue<K> target) {
        KeyComparison<K> comparison = new KeyComparison<>();
        comparison.setSource(source);
        comparison.setTarget(target);
        comparison.setStatus(status(source, target));
        return comparison;
    }

    private Status status(KeyValue<K> source, KeyValue<K> target) {
        if (KeyEvent.TYPE_NONE.equalsIgnoreCase(target.getType())) {
            if (KeyEvent.TYPE_NONE.equalsIgnoreCase(source.getType())) {
                return Status.OK;
            }
            return Status.MISSING;
        }
        if (!source.getType().equals(target.getType())) {
            return Status.TYPE;
        }
        if (!ttlEquals(source, target)) {
            return Status.TTL;
        }
        if (!valueEquals(source, target)) {
            return Status.VALUE;
        }
        return Status.OK;
    }

    private boolean ttlEquals(KeyValue<K> source, KeyValue<K> target) {
        if (source.getTtl() == null) {
            return target.getTtl() == null;
        }
        if (target.getTtl() == null) {
            return false;
        }
        return Math.abs(source.getTtl().toEpochMilli() - target.getTtl().toEpochMilli()) <= ttlTolerance.toMillis();
    }

    @SuppressWarnings("unchecked")
    private boolean valueEquals(KeyValue<K> source, KeyValue<K> target) {
        Object a = source.getValue();
        Object b = target.getValue();
        if (a == b) {
            return true;
        } else {
            if (a == null || b == null) {
                return false;
            }
        }
        if (KeyValue.TYPE_NONE.equalsIgnoreCase(source.getType())) {
            return KeyValue.TYPE_NONE.equalsIgnoreCase(target.getType());
        }
        String type = source.getType();
        if (type == null) {
            return Objects.deepEquals(a, b);
        }
        switch (type) {
            case KeyValue.TYPE_JSON:
            case KeyValue.TYPE_STRING:
                return valueEquals((V) a, (V) b);
            case KeyValue.TYPE_HASH:
                return mapEquals((Map<K, V>) a, (Map<K, V>) b);
            case KeyValue.TYPE_LIST:
                return collectionEquals((Collection<V>) a, (Collection<V>) b);
            case KeyValue.TYPE_SET:
                return setEquals((Collection<V>) a, (Collection<V>) b);
            case KeyValue.TYPE_STREAM:
                return streamEquals((Collection<StreamMessage<K, V>>) a, (Collection<StreamMessage<K, V>>) b);
            case KeyValue.TYPE_ZSET:
                return zsetEquals((Collection<ScoredValue<V>>) a, (Collection<ScoredValue<V>>) b);
            default:
                return Objects.deepEquals(a, b);
        }
    }

    private boolean valueEquals(V source, V target) {
        return Objects.equals(hashFunction.apply(source), hashFunction.apply(target));
    }

    private boolean zsetEquals(Collection<ScoredValue<V>> source, Collection<ScoredValue<V>> target) {
        return Objects.deepEquals(hashCodeZset(source), hashCodeZset(target));
    }

    private boolean setEquals(Collection<V> source, Collection<V> target) {
        return Objects.deepEquals(hashCodeSet(source), hashCodeSet(target));
    }

    private boolean collectionEquals(Collection<V> source, Collection<V> target) {
        return Objects.deepEquals(hashCodeList(source), hashCodeList(target));
    }

    private boolean mapEquals(Map<K, V> source, Map<K, V> target) {
        return Objects.deepEquals(hashCodeMap(source), hashCodeMap(target));
    }

    private Set<ScoredValue<Object>> hashCodeZset(Collection<ScoredValue<V>> zset) {
        return zset.stream().map(v -> ScoredValue.just(v.getScore(), hashFunction.apply(v.getValue())))
                .collect(Collectors.toSet());
    }

    private Set<Object> hashCodeSet(Collection<V> set) {
        return set.stream().map(hashFunction).collect(Collectors.toSet());
    }

    private List<Object> hashCodeList(Collection<V> collection) {
        return collection.stream().map(hashFunction).collect(Collectors.toList());
    }

    private Map<Object, Object> hashCodeMap(Map<K, V> map) {
        Map<Object, Object> hashMap = new HashMap<>();
        map.forEach((k, v) -> hashMap.put(hashFunction.apply(k), hashFunction.apply(v)));
        return hashMap;
    }

    private boolean streamEquals(Collection<StreamMessage<K, V>> source, Collection<StreamMessage<K, V>> target) {
        if (CollectionUtils.isEmpty(source)) {
            return CollectionUtils.isEmpty(target);
        }
        if (source.size() != target.size()) {
            return false;
        }
        Iterator<StreamMessage<K, V>> sourceIterator = source.iterator();
        Iterator<StreamMessage<K, V>> targetIterator = target.iterator();
        while (sourceIterator.hasNext()) {
            if (!targetIterator.hasNext()) {
                return false;
            }
            StreamMessage<K, V> sourceMessage = sourceIterator.next();
            StreamMessage<K, V> targetMessage = targetIterator.next();
            if (streamMessageIds && !Objects.equals(sourceMessage.getId(), targetMessage.getId())) {
                return false;
            }
            if (!mapEquals(sourceMessage.getBody(), targetMessage.getBody())) {
                return false;
            }
        }
        return true;
    }

    public Duration getTtlTolerance() {
        return ttlTolerance;
    }

    public void setTtlTolerance(Duration ttlTolerance) {
        this.ttlTolerance = ttlTolerance;
    }

    public boolean isStreamMessageIds() {
        return streamMessageIds;
    }

    public void setStreamMessageIds(boolean enable) {
        this.streamMessageIds = enable;
    }

}
