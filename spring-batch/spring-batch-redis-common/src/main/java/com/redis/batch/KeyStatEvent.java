package com.redis.batch;

import java.time.Instant;

public class KeyStatEvent<K> extends KeyEvent<K> {

    private Instant ttl;

    private Long memoryUsage;

    /**
     * @return expiration time for the key, or null if none set
     */
    public Instant getTtl() {
        return ttl;
    }

    public void setTtl(Instant ttl) {
        this.ttl = ttl;
    }

    public Long getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(Long memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

}
