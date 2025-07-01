package com.redis.batch;

import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A specialized KeyValueEvent with built-in type-safe access to Redis data structures. Provides type checking and safe
 * conversion methods similar to Jackson's JsonNode.
 */
public class KeyStructEvent<K, V> extends KeyTtlTypeEvent<K> {

    private Object value;

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    // Type checking methods - direct implementation

    /**
     * @return true if this is a Redis string value
     */
    public boolean isString() {
        return getType() == KeyType.string;
    }

    /**
     * @return true if this is a Redis hash value
     */
    public boolean isHash() {
        return getType() == KeyType.hash;
    }

    /**
     * @return true if this is a Redis list value
     */
    public boolean isList() {
        return getType() == KeyType.list;
    }

    /**
     * @return true if this is a Redis set value
     */
    public boolean isSet() {
        return getType() == KeyType.set;
    }

    /**
     * @return true if this is a Redis sorted set (zset) value
     */
    public boolean isZset() {
        return getType() == KeyType.zset;
    }

    /**
     * @return true if this is a Redis stream value
     */
    public boolean isStream() {
        return getType() == KeyType.stream;
    }

    /**
     * @return true if this is a Redis time series value
     */
    public boolean isTimeseries() {
        return getType() == KeyType.timeseries;
    }

    /**
     * @return true if this is a Redis JSON value
     */
    public boolean isJson() {
        return getType() == KeyType.json;
    }

    /**
     * @return true if this is a none/null value
     */
    public boolean isNone() {
        return getType() == KeyType.none;
    }

    // Safe conversion methods - direct implementation

    /**
     * Helper method to safely cast values with consistent null handling and error messages
     */
    @SuppressWarnings("unchecked")
    private <T> T castValue(KeyType expectedType) {
        if (value == null) {
            return null;
        }
        if (isNone()) {
            return null;
        }
        if (getType() == expectedType) {
            return (T) value;
        }
        throw new IllegalStateException("Cannot convert " + getType() + " to " + expectedType);
    }

    /**
     * Convert to string value
     *
     * @return the string value
     * @throws IllegalStateException if not a string type
     */
    public V asString() {
        return castValue(KeyType.string);
    }

    /**
     * Convert to hash value
     *
     * @return the hash map
     * @throws IllegalStateException if not a hash type
     */
    public Map<K, V> asHash() {
        return castValue(KeyType.hash);
    }

    /**
     * Convert to list value
     *
     * @return the list
     * @throws IllegalStateException if not a list type
     */
    public List<V> asList() {
        return castValue(KeyType.list);
    }

    /**
     * Convert to set value
     *
     * @return the set
     * @throws IllegalStateException if not a set type
     */
    public Set<V> asSet() {
        return castValue(KeyType.set);
    }

    /**
     * Convert to sorted set value
     *
     * @return the sorted set as scored values
     * @throws IllegalStateException if not a zset type
     */
    public Set<ScoredValue<V>> asZSet() {
        return castValue(KeyType.zset);
    }

    /**
     * Convert to stream value
     *
     * @return the stream messages
     * @throws IllegalStateException if not a stream type
     */
    public List<StreamMessage<K, V>> asStream() {
        return castValue(KeyType.stream);
    }

    /**
     * Convert to time series value
     *
     * @return the time series samples
     * @throws IllegalStateException if not a timeseries type
     */
    public List<Sample> asTimeseries() {
        return castValue(KeyType.timeseries);
    }

    /**
     * Convert to JSON value
     *
     * @return the JSON value
     * @throws IllegalStateException if not a JSON type
     */
    public V asJson() {
        return castValue(KeyType.json);
    }

}
