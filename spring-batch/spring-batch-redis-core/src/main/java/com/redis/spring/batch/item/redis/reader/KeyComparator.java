package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyTtlTypeEvent;

public interface KeyComparator<K, T extends KeyTtlTypeEvent<K>> {

    KeyComparison<K> compare(T source, T target);

}
