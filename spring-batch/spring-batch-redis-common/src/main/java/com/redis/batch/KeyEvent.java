package com.redis.batch;

import java.time.Instant;

public class KeyEvent<K> {

    private K key;

    private Instant timestamp;

    private String event;

    private String type;

    private KeyOperation operation;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public KeyOperation getOperation() {
        return operation;
    }

    public void setOperation(KeyOperation operation) {
        this.operation = operation;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        KeyEvent<?> keyEvent = (KeyEvent<?>) o;
        return BatchUtils.keyEquals(key, keyEvent.key);
    }

    @Override
    public int hashCode() {
        return BatchUtils.keyHashCode(key);
    }

}
