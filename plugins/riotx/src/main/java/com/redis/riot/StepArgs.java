package com.redis.riot;

import com.redis.riot.core.job.StepFactoryBean;
import lombok.ToString;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.time.Duration;

@ToString
public class StepArgs {

    public static final int DEFAULT_CHUNK_SIZE = 50;

    public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;

    @Option(names = "--threads", description = "Number of concurrent threads to use for batch processing (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int threads = StepFactoryBean.DEFAULT_THREADS;

    @Option(names = "--batch", description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    private int chunkSize = DEFAULT_CHUNK_SIZE;

    @Option(names = "--skip-limit", description = "Number of skips tolerated before failing. Use with limit skip policy.", paramLabel = "<int>")
    private int skipLimit;

    @Option(names = "--retry-limit", description = "Number of times to try failed items (default: ${DEFAULT-VALUE}). 0 and 1 both mean no retry. Use with limit retry policy", paramLabel = "<int>")
    private int retryLimit = DEFAULT_RETRY_LIMIT;

    @CommandLine.Option(names = "--sleep", description = "Duration to wait after each batch write, e.g. 1ms or 3s (default: no sleep).", paramLabel = "<dur>")
    private Duration sleep;

    @CommandLine.Option(names = "--dry-run", description = "Enable dummy writes.")
    private boolean dryRun;

    @CommandLine.ArgGroup(exclusive = false)
    private BackOffArgs backOffArgs = new BackOffArgs();

    public BackOffArgs getBackOffArgs() {
        return backOffArgs;
    }

    public void setBackOffArgs(BackOffArgs args) {
        this.backOffArgs = args;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

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

}
