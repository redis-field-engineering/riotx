package com.redis.riotx.stats;

import com.redis.spring.batch.item.redis.common.KeyValue;

/**
 * Statistics for a specific type of Redis key
 */
public class KeyTypeStats {

	private final String type;
	private long count;
	private MetricStats ttlStats = new MetricStats();
	private MetricStats memoryStats = new MetricStats();

	public KeyTypeStats(String type) {
		this.type = type;
	}

	public void add(KeyValue<String> item) {
		count++;
		if (item.getTtl() > 0) {
			ttlStats.add(item.getTtl());
		}
		if (item.getMemoryUsage() > 0) {
			memoryStats.add(item.getMemoryUsage());
		}
	}

	public String getType() {
		return type;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public MetricStats getTtlStats() {
		return ttlStats;
	}

	public void setTtlStats(MetricStats stats) {
		this.ttlStats = stats;
	}

	public MetricStats getMemoryStats() {
		return memoryStats;
	}

	public void setMemoryStats(MetricStats stats) {
		this.memoryStats = stats;
	}

	@Override
	public String toString() {
		return "KeyTypeStats [type=" + type + ", count=" + count + ", ttlStats=" + ttlStats.quantile(.5)
				+ ", memoryStats=" + memoryStats.quantile(.5) + "]";
	}

}