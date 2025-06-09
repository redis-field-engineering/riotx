package com.redis.batch.gen;

import com.redis.batch.Range;

public class StreamOptions {

	public static final Range DEFAULT_MESSAGE_COUNT = new Range(10, 10);

	private Range messageCount = DEFAULT_MESSAGE_COUNT;
	private MapOptions bodyOptions = new MapOptions();

	public Range getMessageCount() {
		return messageCount;
	}

	public void setMessageCount(Range count) {
		this.messageCount = count;
	}

	public MapOptions getBodyOptions() {
		return bodyOptions;
	}

	public void setBodyOptions(MapOptions options) {
		this.bodyOptions = options;
	}

}
