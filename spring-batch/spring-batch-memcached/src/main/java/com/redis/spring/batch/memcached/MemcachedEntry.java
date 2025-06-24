package com.redis.spring.batch.memcached;

import java.time.Instant;

public class MemcachedEntry {

    private String key;

    private Instant timestamp = Instant.now();

    private byte[] value;

    private Instant expiration;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    /**
     * @return Expiration time for this key or null if no expiration set
     */
    public Instant getExpiration() {
        return expiration;
    }

    public void setExpiration(Instant expiration) {
        this.expiration = expiration;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

}
