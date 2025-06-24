package com.redis.batch;

import java.time.Instant;

public class KeyValueEvent<K> extends KeyEvent<K> {

    private Instant ttl;

    private Object value;

    /**
     * @return expiration time for the key, or null if none set
     */
    public Instant getTtl() {
        return ttl;
    }

    public void setTtl(Instant ttl) {
        this.ttl = ttl;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public static <T, K> KeyValueEvent<K> of(K key, KeyValueEvent<?> other) {
        KeyValueEvent<K> copy = new KeyValueEvent<>();
        copy.setKey(key);
        copy.setTimestamp(other.getTimestamp());
        copy.setEvent(other.getEvent());
        copy.setType(other.getType());
        copy.setOperation(other.getOperation());
        copy.setTtl(other.getTtl());
        copy.setValue(other.getValue());
        return copy;
    }

}
