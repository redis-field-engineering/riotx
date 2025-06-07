package com.redis.riot;

import com.redis.riot.core.PrefixedNumber;
import com.redis.riot.core.job.RiotStep;
import lombok.ToString;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@ToString
public class StepArgs implements StepConfigurer {

    public static final int DEFAULT_RETRY_LIMIT = 10;

    @Option(names = "--threads", defaultValue = "${RIOT_THREADS:-1}", description = "Number of concurrent threads to use for batch processing (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int threads = RiotStep.DEFAULT_THREADS;

    @Option(names = "--batch", defaultValue = "${RIOT_BATCH:-50}", description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    private int chunkSize = RiotStep.DEFAULT_COMMIT_INTERVAL;

    @CommandLine.Option(names = "--rate", defaultValue = "${RIOT_RATE}", description = "Limit number of items written per second, for example 100 or 5k (default: no limit).", paramLabel = "<int>")
    private PrefixedNumber writesPerSecond = new PrefixedNumber();

    @CommandLine.Option(names = "--dry-run", defaultValue = "${RIOT_DRY_RUN}", description = "Enable dummy writes.")
    private boolean dryRun;

    @CommandLine.Option(names = "--skip", defaultValue = "${RIOT_SKIP}", description = "Number of failed items before failing the job (default: no skipping).", paramLabel = "<int>")
    private int skipLimit;

    @CommandLine.Option(names = "--retry", defaultValue = "${RIOT_RETRY:-10}", description = "Number of times to retry failed items: 0 -> never, -1 -> always (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int retryLimit = DEFAULT_RETRY_LIMIT;

    @CommandLine.ArgGroup(exclusive = false)
    private BackOffArgs backOffArgs = new BackOffArgs();

    @Override
    public void configure(RiotStep<?, ?> step) {
        step.setDryRun(dryRun);
        step.setWritesPerSecond(writesPerSecond.intValue());
        step.setThreads(threads);
        step.setCommitInterval(chunkSize);
        step.setRetryLimit(retryLimit);
        step.setSkipLimit(skipLimit);
        step.setBackOffPolicy(backOffArgs.backOffPolicy());
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public PrefixedNumber getWritesPerSecond() {
        return writesPerSecond;
    }

    public void setWritesPerSecond(PrefixedNumber writesPerSecond) {
        this.writesPerSecond = writesPerSecond;
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

    public BackOffArgs getBackOffArgs() {
        return backOffArgs;
    }

    public void setBackOffArgs(BackOffArgs backOffArgs) {
        this.backOffArgs = backOffArgs;
    }

}
