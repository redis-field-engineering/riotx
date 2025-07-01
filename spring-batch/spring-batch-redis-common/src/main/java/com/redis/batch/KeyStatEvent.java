package com.redis.batch;

public class KeyStatEvent<K> extends KeyTtlTypeEvent<K> {

    private Long memoryUsage;

    public Long getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(Long memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

}
