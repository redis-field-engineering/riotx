package com.redis.spring.batch.item.redis.gen;

import com.redis.spring.batch.item.redis.common.KeyValue;

public enum ItemType {

	HASH(KeyValue.TYPE_HASH), JSON(KeyValue.TYPE_JSON), LIST(KeyValue.TYPE_LIST), SET(KeyValue.TYPE_SET),
	STREAM(KeyValue.TYPE_STREAM), STRING(KeyValue.TYPE_STRING), TIMESERIES(KeyValue.TYPE_TIMESERIES),
	ZSET(KeyValue.TYPE_ZSET);

	private final String string;

	private ItemType(String string) {
		this.string = string;
	}

	public String getString() {
		return string;
	}

}