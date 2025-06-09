package com.redis.batch;

import java.util.Arrays;

public class Key<K> {

	protected K key;

	public Key() {
	}

	public Key(K key) {
		this.key = key;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	@Override
	public int hashCode() {
		if (key instanceof byte[]) {
			return Arrays.hashCode((byte[]) key);
		}
		return key.hashCode();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof Key)) {
			return false;
		}

		Key<K> that = (Key<K>) obj;

		if (key instanceof byte[] && that.key instanceof byte[]) {
			return Arrays.equals((byte[]) key, (byte[]) that.key);
		}

		return key.equals(that.key);
	}

	@Override
	public String toString() {
		return BatchUtils.toString(key);
	}

}
