package com.redis.riot;

import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.item.redis.reader.MemoryUsage;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class MemoryUsageArgs {

    public static final int DEFAULT_SAMPLES = MemoryUsage.DEFAULT_SAMPLES;

    public static final DataSize DEFAULT_LIMIT = MemoryUsage.NO_LIMIT;

    @Option(names = "--mem-limit", description = "Max mem usage for a key to be read, for example 12KB 5MB. Disabled by default. Use 0 for no limit but still read mem usage.", paramLabel = "<size>")
    private DataSize limit = DEFAULT_LIMIT;

    @Option(names = "--mem-samples", description = "Number of memory usage samples for a key (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int samples = DEFAULT_SAMPLES;

    public DataSize getLimit() {
        return limit;
    }

    public void setLimit(DataSize limit) {
        this.limit = limit;
    }

    public int getSamples() {
        return samples;
    }

    public void setSamples(int samples) {
        this.samples = samples;
    }

    public MemoryUsage memoryUsage() {
        return MemoryUsage.of(limit, samples);
    }

}
