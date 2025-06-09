package com.redis.batch.gen;

import com.redis.batch.Range;

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
