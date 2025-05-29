package com.redis.riot.core;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.item.redis.common.KeyValue;

public class KeyValueFilter<K> implements ItemProcessor<KeyValue<K>, KeyValue<K>> {

    private final long memoryLimit;

    public KeyValueFilter(long memoryLimit) {
        Assert.isTrue(memoryLimit > 0, "memoryLimit must be greater than 0");
        this.memoryLimit = memoryLimit;
    }

    @Override
    public KeyValue<K> process(KeyValue<K> item) throws Exception {
        if (KeyValue.exists(item) && item.getMemoryUsage() > memoryLimit) {
            return null;
        }
        return item;
    }

}
