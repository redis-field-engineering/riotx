package com.redis.spring.batch;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

import org.springframework.batch.core.observability.BatchMetrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

/**
 * Central class for batch Redis metrics. It provides:
 *
 * <ul>
 * <li>the main entry point to interact with Micrometer's API to create common
 * metrics such as {@link Timer} and {@link LongTaskTimer}.</li>
 * <li>Some utility methods like calculating durations and formatting them in a
 * human readable format.</li>
 * </ul>
 *
 */
public final class BatchRedisMetrics {

	public static final String METRICS_PREFIX = BatchMetrics.METRICS_PREFIX + "redis.";
	public static final String SIZE_SUFFIX = ".size";
	public static final String CAPACITY_SUFFIX = ".capacity";
	public static final String TAG_NONE = "none";

	private BatchRedisMetrics() {
	}

	public static <T extends BlockingQueue<?>> T gaugeQueue(MeterRegistry meterRegistry, String name, T queue,
			Tag... tags) {
		Gauge.builder(METRICS_PREFIX + name + SIZE_SUFFIX, queue, BlockingQueue::size).tags(Arrays.asList(tags))
				.description("Gauge reflecting the size (depth) of the queue").register(meterRegistry);
		Gauge.builder(METRICS_PREFIX + name + CAPACITY_SUFFIX, queue, BlockingQueue::remainingCapacity)
				.tags(Arrays.asList(tags)).description("Gauge reflecting the remaining capacity of the queue")
				.register(meterRegistry);
		return queue;
	}

	/**
	 * Create a {@link Timer}.
	 * 
	 * @param meterRegistry the meter registry to use
	 * @param name          of the timer. Will be prefixed with
	 *                      {@link BatchMetrics#METRICS_PREFIX}.
	 * @param description   of the timer
	 * @param tags          of the timer
	 * @return a new timer instance
	 */
	public static Timer createTimer(MeterRegistry meterRegistry, String name, String description, Tag... tags) {
		return Timer.builder(METRICS_PREFIX + name).description(description).tags(Arrays.asList(tags))
				.register(meterRegistry);
	}

	/**
	 * Create a {@link Counter}.
	 * 
	 * @param meterRegistry the meter registry to use
	 * @param name          of the counter. Will be prefixed with
	 *                      {@link BatchMetrics#METRICS_PREFIX}.
	 * @param description   of the counter
	 * @param tags          of the counter
	 * @return a new timer instance
	 */
	public static Counter createCounter(MeterRegistry meterRegistry, String name, String description, Tag... tags) {
		return createCounter(meterRegistry, name, description, Arrays.asList(tags));
	}

	public static Counter createCounter(MeterRegistry meterRegistry, String name, String description,
			Iterable<Tag> tags) {
		return Counter.builder(METRICS_PREFIX + name).description(description).tags(tags).register(meterRegistry);
	}

	public static Gauge createGauge(MeterRegistry meterRegistry, String name, Supplier<Number> f, String description,
			Tag... tags) {
		return Gauge.builder(METRICS_PREFIX + name, f).description(description).tags(Arrays.asList(tags))
				.register(meterRegistry);
	}

	public static String tagValue(String value) {
		return value == null ? TAG_NONE : value;
	}

}
