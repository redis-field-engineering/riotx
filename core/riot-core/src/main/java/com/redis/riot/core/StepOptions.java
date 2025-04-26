package com.redis.riot.core;

import lombok.ToString;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;

import java.time.Duration;

@ToString
public class StepOptions {

    public static final int DEFAULT_CHUNK_SIZE = 50;

    public static final int DEFAULT_THREADS = 1;

    public static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicy.NEVER;

    public static final SkipPolicy DEFAULT_SKIP_POLICY = SkipPolicy.NEVER;

    public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;

    private Duration sleep;

    private int threads = DEFAULT_THREADS;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private boolean dryRun;

    private SkipPolicy skipPolicy = DEFAULT_SKIP_POLICY;

    private int skipLimit;

    private RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;

    private int retryLimit = DEFAULT_RETRY_LIMIT;

    public Duration getSleep() {
        return sleep;
    }

    public void setSleep(Duration sleep) {
        this.sleep = sleep;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public int getSkipLimit() {
        return skipLimit;
    }

    public void setSkipLimit(int skipLimit) {
        this.skipLimit = skipLimit;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public void setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
    }

    public SkipPolicy getSkipPolicy() {
        return skipPolicy;
    }

    public void setSkipPolicy(SkipPolicy skipPolicy) {
        this.skipPolicy = skipPolicy;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

}
