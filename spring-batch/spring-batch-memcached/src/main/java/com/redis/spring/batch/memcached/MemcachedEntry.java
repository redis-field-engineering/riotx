package com.redis.spring.batch.memcached;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

public class MemcachedEntry {

    private String key;

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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        MemcachedEntry that = (MemcachedEntry) o;
        return Objects.equals(key, that.key) && Objects.deepEquals(value, that.value) && Objects.equals(expiration,
                that.expiration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, Arrays.hashCode(value), expiration);
    }

}
