package com.redis.spring.batch.item.redis.reader;

import org.springframework.util.unit.DataSize;

import lombok.ToString;

@ToString
public class MemoryUsage {

    public static final DataSize DISABLED = DataSize.ofBytes(-1);

    public static final DataSize NO_LIMIT = DataSize.ofBytes(0);

    public static final DataSize DEFAULT_LIMIT = DISABLED;

    public static final int DEFAULT_SAMPLES = 5;

    private DataSize limit = DEFAULT_LIMIT;

    private int samples = DEFAULT_SAMPLES;

    public DataSize getLimit() {
        return limit;
    }

    public void setLimit(DataSize limit) {
        this.limit = limit;
    }

    /**
     * 
     * @return Max memory usage in bytes
     */
    public int getSamples() {
        return samples;
    }

    public void setSamples(int samples) {
        this.samples = samples;
    }

    public static MemoryUsage of(DataSize limit) {
        MemoryUsage usage = new MemoryUsage();
        usage.setLimit(limit);
        return usage;
    }

    public static MemoryUsage of(DataSize limit, int samples) {
        MemoryUsage usage = of(limit);
        usage.setSamples(samples);
        return usage;
    }

}
