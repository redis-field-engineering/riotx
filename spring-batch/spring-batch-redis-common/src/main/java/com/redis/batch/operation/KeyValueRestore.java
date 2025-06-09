package com.redis.batch.operation;

import com.redis.batch.KeyValue;

public class KeyValueRestore<K, V> extends Restore<K, V, KeyValue<K>> {

	public KeyValueRestore() {
		super(KeyValue::getKey, v -> (byte[]) v.getValue());
		setTtlFunction(KeyValue::getTtl);
	}

}
