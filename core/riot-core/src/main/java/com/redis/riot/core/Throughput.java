package com.redis.riot.core;

import java.util.function.LongSupplier;

import org.springframework.util.Assert;

/**
 * 
 * Class for measuring the throughput based on count and measurement period.
 * 
 */
public class Throughput {

	private static final long NOT_TRACKED = -1;
	private final LongSupplier clock;
	private static final long MILLIS_IN_SECOND = 1000;
	private long currentThroughput;

	private long currentAccumulatedCount;
	private long currentMeasurementTime;
	private long measurementStartTime = NOT_TRACKED;

	public Throughput() {
		this(System::currentTimeMillis);
	}

	public Throughput(LongSupplier clock) {
		this.clock = clock;
	}

	public void add(long count) {
		// Force resuming measurement.
		if (measurementStartTime == NOT_TRACKED) {
			measurementStartTime = clock.getAsLong();
		}
		currentAccumulatedCount += count;
	}

	/**
	 * 
	 * Mark when the time should not be taken into account.
	 * 
	 */
	public void pauseMeasurement() {
		if (measurementStartTime != NOT_TRACKED) {
			currentMeasurementTime += clock.getAsLong() - measurementStartTime;
		}
		measurementStartTime = NOT_TRACKED;
	}

	/**
	 * 
	 * Mark when the time should be included to the throughput calculation.
	 * 
	 */
	public void resumeMeasurement() {
		if (measurementStartTime == NOT_TRACKED) {
			measurementStartTime = clock.getAsLong();
		}
	}

	/**
	 * 
	 * @return Calculated throughput based on the collected data for the last
	 *         period.
	 * 
	 */
	public long calculateThroughput() {
		if (measurementStartTime != NOT_TRACKED) {
			long absoluteTimeMillis = clock.getAsLong();
			currentMeasurementTime += absoluteTimeMillis - measurementStartTime;
			measurementStartTime = absoluteTimeMillis;
		}

		long throughput = calculateThroughput(currentAccumulatedCount, currentMeasurementTime);

		currentAccumulatedCount = currentMeasurementTime = 0;

		return throughput;
	}

	public long calculateThroughput(long count, long time) {
		Assert.isTrue(count >= 0, "Count should be positive");
		Assert.isTrue(time >= 0, "Time should be positive");

		if (time == 0) {
			return currentThroughput;
		}

		return currentThroughput = instantThroughput(count, time);
	}

	static long instantThroughput(long count, long time) {
		return (long) ((double) count / time * MILLIS_IN_SECOND);
	}
}
