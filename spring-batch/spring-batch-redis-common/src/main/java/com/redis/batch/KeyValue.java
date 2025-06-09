package com.redis.batch;

import java.time.Instant;

public class KeyValue<K> extends KeyEvent<K> {

    private Instant ttl;

    private Object value;

    private long memoryUsage;

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
     * @return remaining time to live in milliseconds of a Redis key that has an expire set
     */
    public Instant getTtl() {
        return ttl;
    }

    public void setTtl(Instant ttl) {
        this.ttl = ttl;
    }

}
