package com.redis.riotx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.util.StringUtils;

import com.redis.riot.AbstractCompareCommand;
import com.redis.riot.CompareMode;
import com.redis.riot.ExportStepHelper;
import com.redis.riot.RedisContext;
import com.redis.riot.RedisWriterArgs;
import com.redis.riot.ReplicateReadLogger;
import com.redis.riot.ReplicateWriteLogger;
import com.redis.riot.core.RiotException;
import com.redis.riot.core.RiotStep;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.RedisInfo;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import com.redis.spring.batch.item.redis.writer.impl.Del;

import io.lettuce.core.codec.ByteArrayCodec;
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

    private static final String COMPARE_STEP_NAME = "compare";

    private static final String SCAN_TASK_NAME = "Scanning";

    private static final String LIVEONLY_TASK_NAME = "Listening";

    private static final String LIVE_TASK_NAME = "Scanning/Listening";

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
        List<RiotStep<?, ?>> steps = new ArrayList<>();
        RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> step = replicateStep();
        steps.add(step);
        if (shouldCompare()) {
            steps.add(compareStep().name(COMPARE_STEP_NAME));
        }
        return job(steps);
    }

    protected void configureTargetRedisWriter(RedisItemWriter<?, ?, ?> writer) {
        super.configureTargetRedisWriter(writer);
        log.info("Configuring target Redis writer with {}", targetRedisWriterArgs);
        targetRedisWriterArgs.configure(writer);
        writer.getRedisSupportCheck().getConsumers().add(this::unsupportedRedis);
    }

    protected RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> replicateStep() {
        RedisScanItemReader<byte[], byte[]> reader = reader();
        configureSourceRedisReader(reader);
        RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> step = new ExportStepHelper(log).step(reader, replicateWriter());
        step.processor(filter());
        step.taskName(taskName(reader));
        step.addReadListener(new ReplicateMetricsReadListener<>());
        if (logKeys) {
            log.info("Adding key logger");
            step.addWriteListener(new ReplicateWriteLogger<>(log, reader.getCodec()));
            step.addReadListener(new ReplicateReadLogger<>(log, reader.getCodec()));
        }
        step.addWriteListener(new ReplicateMetricsWriteListener<>());
        return step;
    }

    protected ItemWriter<KeyValue<byte[]>> replicateWriter() {
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> targetWriter = writer();
        configureTargetRedisWriter(targetWriter);
        ItemWriter<KeyValue<byte[]>> writer = processingWriter(targetWriter);
        if (removeSourceKeys) {
            log.info("Adding source delete writer to replicate writer");
            RedisItemWriter<byte[], byte[], KeyValue<byte[]>> sourceDelete = new RedisItemWriter<>(ByteArrayCodec.INSTANCE,
                    new Del<>(KeyValue::getKey));
            configureSourceRedisWriter(sourceDelete);
            return new CompositeItemWriter<>(writer, sourceDelete);
        }
        return writer;

    }

    private boolean shouldCompare() {
        return compareMode != CompareMode.NONE && !getJobArgs().isDryRun();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected RedisScanItemReader<byte[], byte[]> reader() {
        if (isStruct()) {
            log.info("Creating Redis data-structure reader");
            return RedisScanItemReader.struct(ByteArrayCodec.INSTANCE);
        }
        log.info("Creating Redis dump reader");
        return (RedisScanItemReader) RedisScanItemReader.dump();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer() {
        if (isStruct()) {
            log.info("Creating Redis data-structure writer");
            return RedisItemWriter.struct(ByteArrayCodec.INSTANCE);
        }
        log.info("Creating Redis dump writer");
        return (RedisItemWriter) RedisItemWriter.dump();
    }

    @Override
    protected boolean isStruct() {
        return type == Type.STRUCT;
    }

    private String taskName(RedisScanItemReader<?, ?> reader) {
        switch (reader.getMode()) {
            case SCAN:
                return SCAN_TASK_NAME;
            case LIVEONLY:
                return LIVEONLY_TASK_NAME;
            default:
                return LIVE_TASK_NAME;
        }
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

}
