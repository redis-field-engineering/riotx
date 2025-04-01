package com.redis.spring.batch.item.redis.reader;

import com.redis.spring.batch.item.redis.common.Key;

public class KeyEvent<K> extends Key<K> {

	private String event;
	private long timestamp = System.currentTimeMillis();
	private String type;

	/**
	 * @return the code that originated this event (e.g. scan, del, ...)
	 */
	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	/**
	 * 
	 * @return POSIX time in milliseconds when the event happened
	 */
	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long time) {
		this.timestamp = time;
	}

}
