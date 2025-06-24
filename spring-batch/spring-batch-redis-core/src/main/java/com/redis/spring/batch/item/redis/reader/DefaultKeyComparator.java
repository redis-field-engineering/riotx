package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyType;
import com.redis.batch.KeyValueEvent;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;
import io.lettuce.core.codec.RedisCodec;

import java.time.Duration;

public class DefaultKeyComparator<K, V> implements KeyComparator<K> {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    private final StructValueComparator<K, V> valueComparator;

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    public DefaultKeyComparator(RedisCodec<K, V> codec) {
        valueComparator = new StructValueComparator<>(codec);
    }

    public StructValueComparator<K, V> getValueComparator() {
        return valueComparator;
    }

    @Override
    public KeyComparison<K> compare(KeyValueEvent<K> source, KeyValueEvent<K> target) {
        KeyComparison<K> comparison = new KeyComparison<>();
        comparison.setSource(source);
        comparison.setTarget(target);
        comparison.setStatus(status(source, target));
        return comparison;
    }

    private Status status(KeyValueEvent<K> source, KeyValueEvent<K> target) {
        if (KeyType.isNone(target.getType())) {
            if (KeyType.isNone(source.getType())) {
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
        if (!valueComparator.compare(source, target)) {
            return Status.VALUE;
        }
        return Status.OK;
    }

    private boolean ttlEquals(KeyValueEvent<K> source, KeyValueEvent<K> target) {
        if (source.getTtl() == null) {
            return target.getTtl() == null;
        }
        if (target.getTtl() == null) {
            return false;
        }
        return Math.abs(source.getTtl().toEpochMilli() - target.getTtl().toEpochMilli()) <= ttlTolerance.toMillis();
    }

    public Duration getTtlTolerance() {
        return ttlTolerance;
    }

    public void setTtlTolerance(Duration ttlTolerance) {
        this.ttlTolerance = ttlTolerance;
    }

}
