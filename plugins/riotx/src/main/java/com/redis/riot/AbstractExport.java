package com.redis.riot;

import com.redis.riot.core.KeyValueFilter;
import com.redis.riot.core.RedisContext;
import com.redis.riot.core.job.RiotStep;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.step.FlushingChunkProvider;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.unit.DataSize;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.time.Duration;

public abstract class AbstractExport extends AbstractJobCommand {

    private static final String VAR_SOURCE = "source";

    @ArgGroup(exclusive = false)
    private FlushingStepArgs flushingStepArgs = new FlushingStepArgs();

    @ArgGroup(exclusive = false)
    private RedisReaderArgs readerArgs = new RedisReaderArgs();

    @Option(names = "--mem-limit", description = "Max mem usage for a key to be read, for example 12KB 5MB.", paramLabel = "<size>")
    private DataSize memoryLimit;

    @Option(names = "--mem-samples", description = "Number of nested values to sample in key memory usage.", paramLabel = "<int>")
    private int memoryUsageSamples;

    private RedisContext sourceRedisContext;

    protected <K> ItemProcessor<KeyValue<K>, KeyValue<K>> keyValueFilter() {
        if (memoryLimit != null && memoryLimit.toBytes() > 0) {
            return new KeyValueFilter<>(memoryLimit.toBytes());
        }
        return null;
    }

    @Override
    protected <I, O> RiotStep<I, O> step(String name, ItemReader<I> reader, ItemWriter<O> writer) {
        RiotStep<I, O> step = super.step(name, reader, writer);
        flushingStepArgs.configure(step);
        return step;
    }

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        sourceRedisContext = sourceRedisContext();
        sourceRedisContext.afterPropertiesSet();
    }

    protected void configureSource(RedisItemReader<?, ?> reader) {
        sourceRedisContext.configure(reader);
        readerArgs.configure(reader);
        reader.setMemoryLimit(memoryLimit);
        reader.setMemoryUsageSamples(memoryUsageSamples);
    }

    @Override
    protected void teardown() {
        if (sourceRedisContext != null) {
            sourceRedisContext.close();
        }
        super.teardown();
    }

    protected void configure(StandardEvaluationContext context) {
        context.setVariable(VAR_SOURCE, sourceRedisContext.getConnection().sync());
    }

    protected void configureSource(RedisItemWriter<?, ?, ?> writer) {
        sourceRedisContext.configure(writer);
    }

    protected abstract RedisContext sourceRedisContext();

    public RedisReaderArgs getReaderArgs() {
        return readerArgs;
    }

    public void setReaderArgs(RedisReaderArgs args) {
        this.readerArgs = args;
    }

    public FlushingStepArgs getFlushingStepArgs() {
        return flushingStepArgs;
    }

    public void setFlushingStepArgs(FlushingStepArgs flushingStepArgs) {
        this.flushingStepArgs = flushingStepArgs;
    }

    public DataSize getMemoryLimit() {
        return memoryLimit;
    }

    public void setMemoryLimit(DataSize limit) {
        this.memoryLimit = limit;
    }

    public int getMemoryUsageSamples() {
        return memoryUsageSamples;
    }

    public void setMemoryUsageSamples(int samples) {
        this.memoryUsageSamples = samples;
    }

}
