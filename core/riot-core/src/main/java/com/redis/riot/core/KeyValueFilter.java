package com.redis.riot.core;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.item.redis.common.KeyValue;

public class KeyValueFilter<K> implements ItemProcessor<KeyValue<K>, KeyValue<K>> {

    private long memoryLimit;

    public void setMemoryLimit(DataSize limit) {
        if (limit != null) {
            setMemoryLimit(limit.toBytes());
        }
    }

    public void setMemoryLimit(long bytes) {
        this.memoryLimit = bytes;
    }

    @Override
    public KeyValue<K> process(KeyValue<K> item) throws Exception {
        if (KeyValue.exists(item) && memoryLimit > 0 && item.getMemoryUsage() > memoryLimit) {
            return null;
        }
        return item;
    }

}
