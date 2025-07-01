package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyTtlTypeEvent;

public class KeyComparison<K> {

    public enum Status {
        OK, // No difference
        MISSING, // Key missing in target database
        TYPE, // Type mismatch
        TTL, // TTL mismatch
        VALUE // Value mismatch
    }

    private KeyTtlTypeEvent<K> source;

    private KeyTtlTypeEvent<K> target;

    private Status status;

    public KeyTtlTypeEvent<K> getSource() {
        return source;
    }

    public void setSource(KeyTtlTypeEvent<K> source) {
        this.source = source;
    }

    public KeyTtlTypeEvent<K> getTarget() {
        return target;
    }

    public void setTarget(KeyTtlTypeEvent<K> target) {
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
