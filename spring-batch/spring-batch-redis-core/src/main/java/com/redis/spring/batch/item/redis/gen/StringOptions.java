package com.redis.spring.batch.item.redis.gen;

import com.redis.spring.batch.item.redis.common.Range;

public class StringOptions {

	public static final Range DEFAULT_LENGTH = new Range(100, 100);

	private Range length = DEFAULT_LENGTH;

	public Range getLength() {
		return length;
	}

	public void setLength(Range length) {
		this.length = length;
	}

}
