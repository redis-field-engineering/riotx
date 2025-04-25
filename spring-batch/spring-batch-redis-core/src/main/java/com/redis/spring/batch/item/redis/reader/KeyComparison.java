package com.redis.spring.batch.item.redis.reader;

import com.redis.spring.batch.item.redis.common.KeyValue;

public class KeyComparison<K> {

	public enum Status {
		OK, // No difference
		MISSING, // Key missing in target database
		TYPE, // Type mismatch
		TTL, // TTL mismatch
		VALUE // Value mismatch
	}

	private KeyValue<K> source;
	private KeyValue<K> target;
	private Status status;

	public KeyValue<K> getSource() {
		return source;
	}

	public void setSource(KeyValue<K> source) {
		this.source = source;
	}

	public KeyValue<K> getTarget() {
		return target;
	}

	public void setTarget(KeyValue<K> target) {
		this.target = target;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	@Override
	public String toString() {
		return "KeyComparison [source=" + source + ", target=" + target + ", status=" + status + "]";
	}

}
