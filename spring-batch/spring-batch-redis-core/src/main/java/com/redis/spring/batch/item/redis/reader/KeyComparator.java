package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyValueEvent;

public interface KeyComparator<K> {

    KeyComparison<K> compare(KeyValueEvent<K> source, KeyValueEvent<K> target);

}
