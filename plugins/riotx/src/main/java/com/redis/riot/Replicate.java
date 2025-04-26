package com.redis.riot;

import java.io.IOException;

import com.redis.riot.core.*;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.StringUtils;

import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.RedisInfo;
import com.redis.spring.batch.item.redis.common.RedisOperation;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;
import com.redis.spring.batch.item.redis.reader.RedisLiveItemReader;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import com.redis.spring.batch.item.redis.writer.KeyValueRestore;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite;
import com.redis.spring.batch.item.redis.writer.impl.Del;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", aliases = "sync", description = "Replicate a Redis database into another Redis database.")
public class Replicate extends AbstractCompareCommand {

    public enum Type {
        STRUCT, DUMP
    }

    public static final Type DEFAULT_TYPE = Type.DUMP;

    public static final CompareMode DEFAULT_COMPARE_MODE = CompareMode.QUICK;

    private static final String SCAN_TASK_NAME = "Scanning";

    private static final String LIVE_TASK_NAME = "Listening";

    public static final ReplicationMode DEFAULT_REPLICATION_MODE = ReplicationMode.SCAN;

    @Option(names = "--mode", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private ReplicationMode mode = DEFAULT_REPLICATION_MODE;

    @Option(names = "--type", description = "Replication type: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    private Type type = DEFAULT_TYPE;

    @ArgGroup(exclusive = false)
    private RedisWriterArgs targetRedisWriterArgs = new RedisWriterArgs();

    @Option(names = "--log-keys", description = "Log keys being read and written.")
    private boolean logKeys;

    @Option(names = "--compare", description = "Compare mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
    private CompareMode compareMode = DEFAULT_COMPARE_MODE;

    @Option(names = "--struct", description = "Enable data structure-specific replication")
    public void setStruct(boolean enable) {
        this.type = enable ? Type.STRUCT : Type.DUMP;
    }

    @Override
    protected boolean isQuickCompare() {
        return compareMode == CompareMode.QUICK;
    }

    @Override
    protected Job job() {
        FlowBuilder<SimpleFlow> flow = flow();
        if (shouldCompare()) {
            flow.next(step(compareStep()).build());
        }
        return job(flow.build());
    }

    public FlowBuilder<SimpleFlow> flow() {
        switch (mode) {
            case LIVE:
                return flow("replicationFlow").split(asyncTaskExecutor()).add(scanFlow().build(), liveFlow().build());
            case LIVEONLY:
                return liveFlow();
            default:
                return scanFlow();
        }
    }

    private FlowBuilder<SimpleFlow> scanFlow() {
        return flow("scanFlow").start(step(scanStep()).build());
    }

    private FlowBuilder<SimpleFlow> liveFlow() {
        return flow("liveFlow").start(step(liveStep()).build());
    }

    private FlowBuilder<SimpleFlow> flow(String name) {
        return new FlowBuilder<>(name);
    }

    private TaskExecutor asyncTaskExecutor() {
        return new SimpleAsyncTaskExecutor("export");
    }

    @Override
    protected <K, V, T> RedisItemWriter<K, V, T> configureTarget(RedisItemWriter<K, V, T> writer) {
        super.configureTarget(writer);
        log.info("Configuring target Redis writer with {}", targetRedisWriterArgs);
        targetRedisWriterArgs.configure(writer);
        writer.getRedisSupportCheck().getConsumers().add(this::unsupportedRedis);
        return writer;
    }

    protected RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> scanStep() {
        return step("scan", scanReader(), SCAN_TASK_NAME);
    }

    private RedisItemReader<byte[], byte[]> scanReader() {
        return configureSource(new RedisScanItemReader<>(CODEC, readOperation()));
    }

    private RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> step(String name, RedisItemReader<byte[], byte[]> reader,
            String taskName) {
        RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> step = step(name, reader, keyValueFilter(), targetWriter(), taskName);
        step.addReadListener(new ReplicateMetricsReadListener<>());
        if (logKeys) {
            log.info("Adding key logger");
            step.addWriteListener(new ReplicateWriteLogger<>(log, reader.getCodec()));
            step.addReadListener(new ReplicateReadLogger<>(log, reader.getCodec()));
        }
        step.addWriteListener(new ReplicateMetricsWriteListener<>());
        return step;
    }

    protected RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> liveStep() {
        return step("live", new RedisLiveItemReader<>(CODEC, readOperation()), LIVE_TASK_NAME);
    }

    protected ItemWriter<KeyValue<byte[]>> targetWriter() {
        ItemWriter<KeyValue<byte[]>> writer = RiotUtils.writer(processor(), configureTarget(writer(writeOperation())));
        if (removeSourceKeys) {
            log.info("Adding source delete writer to replicate writer");
            RedisItemWriter<byte[], byte[], KeyValue<byte[]>> sourceDelete = writer(new Del<>(KeyValue::getKey));
            configureSource(sourceDelete);
            return new CompositeItemWriter<>(writer, sourceDelete);
        }
        return writer;

    }

    private <T> RedisItemWriter<byte[], byte[], T> writer(RedisOperation<byte[], byte[], T, Object> operation) {
        return new RedisItemWriter<>(CODEC, operation);
    }

    private boolean shouldCompare() {
        return compareMode != CompareMode.NONE && !getJobArgs().isDryRun();
    }

    protected KeyValueRead<byte[], byte[]> readOperation() {
        if (isStruct()) {
            return KeyValueRead.struct(CODEC);
        }
        return KeyValueRead.dump();
    }

    private RedisOperation<byte[], byte[], KeyValue<byte[]>, Object> writeOperation() {
        if (isStruct()) {
            log.info("Creating Redis data-structure writer");
            return new KeyValueWrite<>();
        }
        log.info("Creating Redis dump writer");
        return new KeyValueRestore<>();
    }

    @Override
    protected boolean isStruct() {
        return type == Type.STRUCT;
    }

    @ArgGroup(exclusive = false, heading = "Metrics options%n")
    private MetricsArgs metricsArgs = new MetricsArgs();

    @Option(names = "--remove-source-keys", description = "Delete keys from source after they have been successfully replicated.")
    private boolean removeSourceKeys;

    @Override
    protected void initialize() {
        super.initialize();
        try {
            metricsArgs.configureMetrics();
        } catch (IOException e) {
            throw new RiotException("Could not initialize metrics", e);
        }
    }

    private void unsupportedRedis(RedisInfo info) {
        throw new UnsupportedOperationException(message(info));
    }

    private String message(RedisInfo info) {
        if (StringUtils.hasLength(info.getOS())) {
            return info.getOS();
        }
        return info.getServerName();
    }

    @Override
    protected RedisContext sourceRedisContext() {
        RedisContext context = super.sourceRedisContext();
        metricsArgs.configure(context);
        return context;
    }

    @Override
    protected RedisContext targetRedisContext() {
        RedisContext context = super.targetRedisContext();
        metricsArgs.configure(context);
        return context;
    }

    public RedisWriterArgs getTargetRedisWriterArgs() {
        return targetRedisWriterArgs;
    }

    public void setTargetRedisWriterArgs(RedisWriterArgs redisWriterArgs) {
        this.targetRedisWriterArgs = redisWriterArgs;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public boolean isLogKeys() {
        return logKeys;
    }

    public void setLogKeys(boolean enable) {
        this.logKeys = enable;
    }

    public CompareMode getCompareMode() {
        return compareMode;
    }

    public void setCompareMode(CompareMode compareMode) {
        this.compareMode = compareMode;
    }

    public ReplicationMode getMode() {
        return mode;
    }

    public void setMode(ReplicationMode mode) {
        this.mode = mode;
    }

}
