package com.redis.riotx.stats;

import com.tdunning.math.stats.TDigest;

/**
 * Represents a set of metrics including min, max, average and percentiles
 */
public class MetricStats {

	private long min = Long.MAX_VALUE;
	private long max = Long.MIN_VALUE;
	private TDigest digest = TDigest.createDigest(100);
	private long count;
	private long total;

	public void add(long value) {
		count++;
		min = Math.min(min, value);
		max = Math.max(max, value);
		total += value;
		digest.add(value);
	}

	public long getMin() {
		return min;
	}

	public long getMax() {
		return max;
	}

	public long getAvg() {
		return total / count;
	}

	public double quantile(double q) {
		return digest.quantile(q);
	}

	public long getCount() {
		return count;
	}

}