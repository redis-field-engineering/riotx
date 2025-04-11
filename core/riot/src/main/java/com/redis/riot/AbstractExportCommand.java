package com.redis.riot;

import java.time.temporal.ChronoUnit;

import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.RiotDuration;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.step.FlushingChunkProvider;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractExportCommand extends AbstractJobCommand {

    public static final RedisReaderMode DEFAULT_REDIS_READER_MODE = RedisReaderMode.SCAN;

    public static final RiotDuration DEFAULT_FLUSH_INTERVAL = RiotDuration.of(FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL,
            ChronoUnit.MILLIS);

    private static final String VAR_SOURCE = "source";

    @Option(names = "--mode", description = "Redis reader mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private RedisReaderMode mode = DEFAULT_REDIS_READER_MODE;

    @Option(names = "--flush-interval", description = "Max duration between flushes in live mode (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private RiotDuration flushInterval = DEFAULT_FLUSH_INTERVAL;

    @Option(names = "--idle-timeout", description = "Min duration to consider reader complete in live mode, for example 3s 5m (default: no timeout).", paramLabel = "<dur>")
    private RiotDuration idleTimeout;

    @ArgGroup(exclusive = false)
    private RedisReaderArgs readerArgs = new RedisReaderArgs();

    @ArgGroup(exclusive = false)
    private MemoryUsageArgs memoryUsageArgs = new MemoryUsageArgs();

    private RedisContext sourceRedisContext;

    @Override
    protected void initialize() {
        super.initialize();
        sourceRedisContext = sourceRedisContext();
        sourceRedisContext.afterPropertiesSet();
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
        log.info("Configuring source writer with Redis context");
        sourceRedisContext.configure(writer);
    }

    protected abstract RedisContext sourceRedisContext();

    public RedisReaderMode getMode() {
        return mode;
    }

    public void setMode(RedisReaderMode mode) {
        this.mode = mode;
    }

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

    public RiotDuration getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(RiotDuration interval) {
        this.flushInterval = interval;
    }

    public RiotDuration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(RiotDuration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

}
