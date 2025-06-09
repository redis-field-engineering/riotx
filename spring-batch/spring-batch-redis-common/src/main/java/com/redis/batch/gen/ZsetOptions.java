package com.redis.batch.gen;

import com.redis.batch.Range;

public class ZsetOptions extends CollectionOptions {

	public static final Range DEFAULT_SCORE = new Range(0, 100);

	private Range score = DEFAULT_SCORE;

	public Range getScore() {
		return score;
	}

	public void setScore(Range score) {
		this.score = score;
	}

}
