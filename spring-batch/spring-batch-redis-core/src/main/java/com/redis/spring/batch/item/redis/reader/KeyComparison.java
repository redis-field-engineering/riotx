package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyValueEvent;

public class KeyComparison<K> {

    public enum Status {
        OK, // No difference
        MISSING, // Key missing in target database
        TYPE, // Type mismatch
        TTL, // TTL mismatch
        VALUE // Value mismatch
    }

    private KeyValueEvent<K> source;

    private KeyValueEvent<K> target;

    private Status status;

    public KeyValueEvent<K> getSource() {
        return source;
    }

    public void setSource(KeyValueEvent<K> source) {
        this.source = source;
    }

    public KeyValueEvent<K> getTarget() {
        return target;
    }

    public void setTarget(KeyValueEvent<K> target) {
        this.target = target;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "KeyComparison [source=" + source + ", target=" + target + ", status=" + status + "]";
    }

}
