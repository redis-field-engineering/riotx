package com.redis.riot;

import com.redis.riot.core.StreamLengthBackpressureStatusSupplier;
import com.redis.riot.core.job.RiotStep;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.BackOffPolicyBuilder;
import picocli.CommandLine;

public class DebeziumStreamArgs {

    private static final String DEFAULT_STREAM_PREFIX = "data:";

    public static final long DEFAULT_STREAM_LIMIT = StreamLengthBackpressureStatusSupplier.DEFAULT_LIMIT;

    @CommandLine.Option(names = "--stream-prefix", description = "Key prefix for stream containing change events (default: ${DEFAULT-VALUE}", paramLabel = "<str>")
    private String streamPrefix = DEFAULT_STREAM_PREFIX;

    @CommandLine.Option(names = "--stream-limit", description = "Max length of RDI stream (default: ${DEFAULT-VALUE}). Use 0 for no limit.", paramLabel = "<int>")
    private long streamLimit = DEFAULT_STREAM_LIMIT;

    @CommandLine.ArgGroup(exclusive = false)
    private BackOffArgs backOffArgs = new BackOffArgs();

    public String getStreamPrefix() {
        return streamPrefix;
    }

    public void setStreamPrefix(String streamPrefix) {
        this.streamPrefix = streamPrefix;
    }

    public long getStreamLimit() {
        return streamLimit;
    }

    public void setStreamLimit(long streamLimit) {
        this.streamLimit = streamLimit;
    }

    public BackOffArgs getBackOffArgs() {
        return backOffArgs;
    }

    public void setBackOffArgs(BackOffArgs backOffArgs) {
        this.backOffArgs = backOffArgs;
    }

    private BackOffPolicy backOffPolicy() {
        return switch (backOffArgs.getPolicy()) {
            case EXPONENTIAL -> BackOffPolicyBuilder.newBuilder().delay(backOffArgs.getDelay().toMillis())
                    .maxDelay(backOffArgs.getMaxDelay().toMillis()).multiplier(backOffArgs.getMultiplier()).build();
            case FIXED -> BackOffPolicyBuilder.newBuilder().delay(backOffArgs.getDelay().toMillis()).build();
            default -> null;
        };
    }

    public <O, I> void configure(RiotStep<I, O> step) {
        step.setBackOffPolicy(backOffPolicy());
    }

    public String streamKey(String key) {
        return streamPrefix + key;
    }

}
