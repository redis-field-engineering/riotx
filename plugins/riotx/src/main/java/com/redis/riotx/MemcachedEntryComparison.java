package com.redis.riotx;

import com.redis.spring.batch.memcached.MemcachedEntry;

public class MemcachedEntryComparison {

	public enum Status {
		OK, // No difference
		MISSING, // Key missing in target database
		EXPIRATION, // Expiration mismatch
		VALUE // Value mismatch
	}

	private MemcachedEntry source;
	private MemcachedEntry target;
	private Status status;

	public MemcachedEntry getSource() {
		return source;
	}

	public void setSource(MemcachedEntry source) {
		this.source = source;
	}

	public MemcachedEntry getTarget() {
		return target;
	}

	public void setTarget(MemcachedEntry target) {
		this.target = target;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

}
