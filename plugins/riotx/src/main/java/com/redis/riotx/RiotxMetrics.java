package com.redis.riotx;

import java.time.Duration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class RiotxMetrics {

	public static void latency(MeterRegistry registry, String name, String description, Duration duration, Tags tags) {
		Timer lag = Timer.builder(name).description(description).tags(tags).register(registry);
		lag.record(duration);
	}

}
