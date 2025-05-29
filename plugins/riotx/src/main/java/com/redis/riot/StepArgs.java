package com.redis.riot;

import com.redis.riot.core.PrefixedNumber;
import com.redis.riot.core.job.RiotStep;
import lombok.ToString;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@ToString
public class StepArgs {

    public static final int DEFAULT_CHUNK_SIZE = 50;

    @Option(names = "--threads", description = "Number of concurrent threads to use for batch processing (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int threads = RiotStep.DEFAULT_THREADS;

    @Option(names = "--batch", description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    private int chunkSize = DEFAULT_CHUNK_SIZE;

    @CommandLine.Option(names = "--rate", description = "Limit number of items written per second, for example 100 or 5k (default: no limit).", paramLabel = "<int>")
    private PrefixedNumber writesPerSecond;

    @CommandLine.Option(names = "--dry-run", description = "Enable dummy writes.")
    private boolean dryRun;

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

}
