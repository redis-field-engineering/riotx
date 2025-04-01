package com.redis.spring.batch.item.redis.common;

import com.redis.spring.batch.item.redis.reader.KeyEvent;

public class KeyValue<K> extends KeyEvent<K> {

	public static final String TYPE_NONE = "none";
	public static final String TYPE_HASH = "hash";
	public static final String TYPE_JSON = "ReJSON-RL";
	public static final String TYPE_LIST = "list";
	public static final String TYPE_SET = "set";
	public static final String TYPE_STREAM = "stream";
	public static final String TYPE_STRING = "string";
	public static final String TYPE_TIMESERIES = "TSDB-TYPE";
	public static final String TYPE_ZSET = "zset";

	public static final long TTL_NONE = -1;
	public static final long TTL_NO_KEY = -2;

	private long ttl;
	private Object value;
	private long memoryUsage;

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	/**
	 * 
	 * @return number of bytes that a Redis key and its value require to be stored
	 *         in RAM
	 */
	public long getMemoryUsage() {
		return memoryUsage;
	}

	public void setMemoryUsage(long memUsage) {
		this.memoryUsage = memUsage;
	}

	/**
	 * 
	 * @return remaining time to live in milliseconds of a Redis key that has an
	 *         expire set
	 */
	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	public static boolean exists(KeyValue<?> keyValue) {
		return keyValue != null && keyValue.key != null && keyValue.ttl != TTL_NO_KEY && keyValue.getType() != null
				&& !keyValue.getType().equals(TYPE_NONE);
	}

	@Override
	public String toString() {
		return "KeyValue [event=" + getEvent() + ", timestamp=" + getTimestamp() + ", type=" + getType() + ", ttl="
				+ ttl + ", value=" + String.valueOf(value != null) + ", memoryUsage=" + memoryUsage + ", key="
				+ BatchUtils.toString(key) + "]";
	}

	public static boolean hasTtl(KeyValue<?> keyValue) {
		return keyValue.getTtl() > 0;
	}

	public static boolean hasValue(KeyValue<?> keyValue) {
		return keyValue.getValue() != null;
	}

	public static boolean hasType(KeyValue<?> keyValue) {
		return keyValue.getType() != null;
	}

}
