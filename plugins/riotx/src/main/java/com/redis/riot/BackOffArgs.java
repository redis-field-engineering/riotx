package com.redis.riot;

import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.BackOffPolicyBuilder;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import picocli.CommandLine;

import java.time.Duration;

public class BackOffArgs {

    public enum Policy {
        EXPONENTIAL, FIXED, NONE;
    }

    private static final Policy DEFAULT_POLICY = Policy.EXPONENTIAL;

    public static final Duration DEFAULT_DELAY = Duration.ofMillis(ExponentialBackOffPolicy.DEFAULT_INITIAL_INTERVAL);

    public static final Duration DEFAULT_MAX_DELAY = Duration.ofMillis(ExponentialBackOffPolicy.DEFAULT_MAX_INTERVAL);

    public static final double DEFAULT_MULTIPLIER = ExponentialBackOffPolicy.DEFAULT_MULTIPLIER;

    @CommandLine.Option(names = "--backoff", description = "Backoff policy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    private Policy policy = DEFAULT_POLICY;

    @CommandLine.Option(names = "--backoff-delay", description = "Exponential backoff initial duration or fixed backoff duration (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration delay = DEFAULT_DELAY;

    @CommandLine.Option(names = "--backoff-max", description = "Exponential backoff max duration (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration maxDelay = DEFAULT_MAX_DELAY;

    @CommandLine.Option(names = "--backoff-x", description = "Exponential backoff duration increment for each retry attempt (default: ${DEFAULT-VALUE} i.e. 100%% increase per backoff).", paramLabel = "<num>")
    private double multiplier = DEFAULT_MULTIPLIER;

    public BackOffPolicy backOffPolicy() {
        return switch (policy) {
            case EXPONENTIAL -> BackOffPolicyBuilder.newBuilder().delay(delay.toMillis()).maxDelay(maxDelay.toMillis())
                    .multiplier(multiplier).build();
            case FIXED -> BackOffPolicyBuilder.newBuilder().delay(delay.toMillis()).build();
            default -> null;
        };
    }

    public Policy getPolicy() {
        return policy;
    }

    public void setPolicy(Policy policy) {
        this.policy = policy;
    }

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
