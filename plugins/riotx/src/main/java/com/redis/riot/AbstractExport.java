package com.redis.riot;

import com.redis.riot.core.KeyValueFilter;
import com.redis.riot.core.RedisContext;
import com.redis.riot.core.job.RiotStep;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.step.FlushingChunkProvider;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.time.Duration;

public abstract class AbstractExport extends AbstractJobCommand {

    public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

    public static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;

    private static final String VAR_SOURCE = "source";

    @Option(names = "--flush-interval", description = "Max duration between flushes in live mode (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

    @Option(names = "--idle-timeout", description = "Min duration to consider reader complete in live mode, for example 3s 5m (default: no timeout).", paramLabel = "<dur>")
    private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

    @ArgGroup(exclusive = false)
    private RedisReaderArgs readerArgs = new RedisReaderArgs();

    @ArgGroup(exclusive = false)
    private MemoryUsageArgs memoryUsageArgs = new MemoryUsageArgs();

    private RedisContext sourceRedisContext;

    protected <K> ItemProcessor<KeyValue<K>, KeyValue<K>> keyValueFilter() {
        KeyValueFilter<K> filter = new KeyValueFilter<>();
        filter.setMemoryLimit(memoryUsageArgs.getLimit());
        return filter;
    }

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        sourceRedisContext = sourceRedisContext();
        sourceRedisContext.afterPropertiesSet();
    }

    protected <K, V, R extends RedisItemReader<K, V>> R configureSource(R reader) {
        sourceRedisContext.configure(reader);
        readerArgs.configure(reader);
        reader.setMemoryUsage(memoryUsageArgs.memoryUsage());
        return reader;
    }

    protected <K, V, T> RiotStep<KeyValue<K>, T> step(String name, RedisItemReader<K, V> reader, ItemWriter<T> writer) {
        RiotStep<KeyValue<K>, T> step = super.step(name, reader, writer);
        step.flushInterval(flushInterval);
        step.idleTimeout(idleTimeout);
        configureSource(reader);
        return step;
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

    protected <K, V, T> RedisItemWriter<K, V, T> configureSource(RedisItemWriter<K, V, T> writer) {
        sourceRedisContext.configure(writer);
        return writer;
    }

    protected abstract RedisContext sourceRedisContext();

    public RedisReaderArgs getReaderArgs() {
        return readerArgs;
    }

    public void setReaderArgs(RedisReaderArgs args) {
        this.readerArgs = args;
    }

    public MemoryUsageArgs getMemoryUsageArgs() {
        return memoryUsageArgs;
    }

    public void setMemoryUsageArgs(MemoryUsageArgs args) {
        this.memoryUsageArgs = args;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(Duration interval) {
        this.flushInterval = interval;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

}
