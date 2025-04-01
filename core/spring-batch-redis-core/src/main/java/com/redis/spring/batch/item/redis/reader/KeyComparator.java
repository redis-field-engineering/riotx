package com.redis.spring.batch.item.redis.reader;

import com.redis.spring.batch.item.redis.common.KeyValue;

public interface KeyComparator<K> {

	KeyComparison<K> compare(KeyValue<K> source, KeyValue<K> target);

}