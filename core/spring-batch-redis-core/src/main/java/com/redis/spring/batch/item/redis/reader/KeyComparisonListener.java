package com.redis.spring.batch.item.redis.reader;

public interface KeyComparisonListener<K> {

	void comparison(KeyComparison<K> comparison);

}
