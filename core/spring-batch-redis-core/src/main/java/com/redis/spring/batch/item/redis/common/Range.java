package com.redis.spring.batch.item.redis.common;

import java.util.Objects;

import org.springframework.util.Assert;

public class Range {

	public final static int UPPER_BORDER_NOT_DEFINED = Integer.MAX_VALUE;

	private final int min;
	private final int max;

	public Range(int min, int max) {
		Assert.isTrue(min >= 0, "Min value must be  >= 0");
		Assert.isTrue(min <= max, "Min value should be lower or equal to max value");
		this.min = min;
		this.max = max;
	}

	public int getMin() {
		return min;
	}

	public int getMax() {
		return max;
	}

	public boolean hasMaxValue() {
		return max != UPPER_BORDER_NOT_DEFINED;
	}

	public String toString() {
		return min + "-" + max;
	}

	@Override
	public int hashCode() {
		return Objects.hash(max, min);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Range other = (Range) obj;
		return max == other.max && min == other.min;
	}

}
