package com.redis.spring.batch.item.redis.reader;

import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;

public class KeyComparisonStat {

	private Status status;
	private String type;
	private long count;

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public static KeyComparisonStat of(Status status, String type, long count) {
		KeyComparisonStat stat = new KeyComparisonStat();
		stat.setStatus(status);
		stat.setType(type);
		stat.setCount(count);
		return stat;
	}

}