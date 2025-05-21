package com.redis.riot;

import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import picocli.CommandLine;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class BackOffArgs {

    @CommandLine.Option(names = "--backoff-delay", description = "The initial backoff duration (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration delay = Duration.ofMillis(ExponentialBackOffPolicy.DEFAULT_INITIAL_INTERVAL);

    @CommandLine.Option(names = "--backoff-max", description = "The maximum backoff duration (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration maxDelay = Duration.ofMillis(ExponentialBackOffPolicy.DEFAULT_MAX_INTERVAL);

    @CommandLine.Option(names = "--backoff-mult", description = "Backoff duration increment for each retry attempt (default: ${DEFAULT-VALUE} i.e. 100% increase per backoff).", paramLabel = "<num>")
    private double multiplier = ExponentialBackOffPolicy.DEFAULT_MULTIPLIER;

    public Duration getDelay() {
        return delay;
    }

    public void setDelay(Duration delay) {
        this.delay = delay;
    }

    public Duration getMaxDelay() {
        return maxDelay;
    }

    public void setMaxDelay(Duration maxDelay) {
        this.maxDelay = maxDelay;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(double multiplier) {
        this.multiplier = multiplier;
    }

}
