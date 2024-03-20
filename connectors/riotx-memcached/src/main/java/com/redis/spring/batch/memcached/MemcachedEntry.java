package com.redis.spring.batch.memcached;

import java.util.Arrays;
import java.util.Objects;

public class MemcachedEntry {

	private String key;
	private byte[] value;
	/**
	 * Expiration POSIX time in seconds for this key.
	 */
	private int expiration;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public int getExpiration() {
		return expiration;
	}

	public void setExpiration(int ttl) {
		this.expiration = ttl;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(value);
		result = prime * result + Objects.hash(expiration, key);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MemcachedEntry other = (MemcachedEntry) obj;
		return expiration == other.expiration && Objects.equals(key, other.key) && Arrays.equals(value, other.value);
	}

}