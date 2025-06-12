package com.redis.batch;

import java.time.Instant;

public class KeyValue<K> {

    private K key;

    private String event;

    private Instant time = Instant.now();

    private String type;

    private Instant ttl;

    private Object value;

    private long memoryUsage;

    public KeyType type() {
        return KeyType.of(type);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        KeyValue<?> keyValue = (KeyValue<?>) o;
        return BatchUtils.keyEquals(key, keyValue.key);
    }

    @Override
    public int hashCode() {
        return BatchUtils.keyHashCode(key);
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    /**
     * @return number of bytes that a Redis key and its value require to be stored in RAM
     */
    public long getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(long memUsage) {
        this.memoryUsage = memUsage;
    }

    /**
     * @return expiration time for the key, or null if none set
     */
    public Instant getTtl() {
        return ttl;
    }

    public void setTtl(Instant ttl) {
        this.ttl = ttl;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public Instant getTime() {
        return time;
    }

    public void setTime(Instant time) {
        this.time = time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

}
