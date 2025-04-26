package com.redis.riot;

import java.time.Duration;

import com.redis.riot.core.KeyValueFilter;
import com.redis.riot.core.RedisContext;
import com.redis.riot.core.RiotStep;
import com.redis.riot.core.RiotUtils;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import com.redis.spring.batch.step.FlushingChunkProvider;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

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
    protected void initialize() {
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

    protected <K, V, T> RiotStep<KeyValue<K>, T> step(String name, RedisItemReader<K, V> reader,
            ItemProcessor<KeyValue<K>, T> processor, ItemWriter<T> writer, String taskName) {
        RiotStep<KeyValue<K>, T> step = new RiotStep<>(name, reader, writer);
        step.flushInterval(flushInterval);
        step.idleTimeout(idleTimeout);
        configureSource(reader);
        step.taskName(taskName);
        step.processor(RiotUtils.processor(keyValueFilter(), processor));
        if (reader instanceof RedisScanItemReader) {
            step.maxItemCount(BatchUtils.scanSizeEstimator((RedisScanItemReader<K, V>) reader));
        }
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
