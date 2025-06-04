package com.redis.spring.batch.memcached;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

public class MemcachedEntry {

	private String key;
	private byte[] value;
	private Instant expiration;

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

	/**
	 * @return Expiration time for this key or null if no expiration set
	 */
	public Instant getExpiration() {
		return expiration;
	}

	public void setExpiration(Instant expiration) {
		this.expiration = expiration;
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
