package com.redis.riot.replicate;

import com.redis.riot.AbstractCompareCommand;
import com.redis.riot.MetricsArgs;
import com.redis.riot.RedisWriterArgs;
import com.redis.riot.core.CompareMode;
import com.redis.riot.core.RedisContext;
import com.redis.riot.core.ReplicationMode;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.job.FlowFactoryBean;
import com.redis.riot.core.job.RiotStep;
import com.redis.riot.core.job.StepFlowFactoryBean;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.RedisInfo;
import com.redis.spring.batch.item.redis.common.RedisOperation;
import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;
import com.redis.spring.batch.item.redis.reader.RedisLiveItemReader;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import com.redis.spring.batch.item.redis.writer.KeyValueRestore;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite;
import com.redis.spring.batch.item.redis.writer.impl.Del;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.util.StringUtils;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Arrays;

@Command(name = "replicate", aliases = "sync", description = "Replicate a Redis database into another Redis database.")
public class Replicate extends AbstractCompareCommand {

    public enum Type {
        STRUCT, DUMP
    }

    private static final String SCAN_STEP = "scanStep";

    private static final String LIVE_STEP = "liveStep";

    public static final Type DEFAULT_TYPE = Type.DUMP;

    public static final CompareMode DEFAULT_COMPARE_MODE = CompareMode.QUICK;

    private static final String SCAN_TASK_NAME = "Scanning";

    private static final String LIVE_TASK_NAME = "Listening";

    public static final ReplicationMode DEFAULT_REPLICATION_MODE = ReplicationMode.SCAN;

    @Option(names = "--mode", defaultValue = "${RIOT_SYNC_MODE:-SCAN}", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private ReplicationMode mode = DEFAULT_REPLICATION_MODE;

    @Option(names = "--struct", defaultValue = "${RIOT_SYNC_STRUCT}", description = "Enable data structure-specific replication.")
    private boolean struct;

    @ArgGroup(exclusive = false)
    private RedisWriterArgs targetRedisWriterArgs = new RedisWriterArgs();

    @Option(names = "--log-keys", defaultValue = "${RIOT_LOG_KEYS}", description = "Log keys being read and written.")
    private boolean logKeys;

    @Option(names = "--compare", defaultValue = "${RIOT_COMPARE:-QUICK}", description = "Compare mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
    private CompareMode compareMode = DEFAULT_COMPARE_MODE;

    @Override
    protected boolean isQuickCompare() {
        return compareMode == CompareMode.QUICK;
    }

    @ArgGroup(exclusive = false, heading = "Metrics options%n")
    private MetricsArgs metricsArgs = new MetricsArgs();

    @Option(names = "--remove-source-keys", defaultValue = "${RIOT_REMOVE_SOURCE_KEYS}", description = "Delete keys from source after they have been successfully replicated.")
    private boolean removeSourceKeys;

    @Override
    protected Job job() throws Exception {
        return job(flow());
    }

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return switch (step.getName()) {
            case SCAN_STEP -> SCAN_TASK_NAME;
            case LIVE_STEP -> LIVE_TASK_NAME;
            default -> super.taskName(step);
        };
    }

    private FlowFactoryBean flow() {
        FlowFactoryBean replicateFlow = replicateFlow();
        if (shouldCompare()) {
            StepFlowFactoryBean<KeyComparison<byte[]>, KeyComparison<byte[]>> compareFlow = new StepFlowFactoryBean<>(
                    compareStep());
            return FlowFactoryBean.sequential("replicateCompareFlow", Arrays.asList(replicateFlow, compareFlow));
        }
        return replicateFlow;
    }

    public FlowFactoryBean replicateFlow() {
        return switch (mode) {
            case LIVE -> FlowFactoryBean.parallel("scanLiveFlow", scanFlow(), liveFlow());
            case LIVEONLY -> liveFlow();
            default -> scanFlow();
        };
    }

    private FlowFactoryBean scanFlow() {
        return new StepFlowFactoryBean<>(scanStep());
    }

    private FlowFactoryBean liveFlow() {
        return new StepFlowFactoryBean<>(liveStep());
    }

    @Override
    protected void configureTarget(RedisItemWriter<?, ?, ?> writer) {
        super.configureTarget(writer);
        log.info("Configuring target Redis writer with {}", targetRedisWriterArgs);
        targetRedisWriterArgs.configure(writer);
        writer.getRedisSupportCheck().getConsumers().add(this::unsupportedRedis);
    }

    private RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> replicateStep(String name, RedisItemReader<byte[], byte[]> reader) {
        configureSource(reader);
        RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> step = step(name, reader, targetWriter());
        step.setItemProcessor(keyValueFilter());
        step.addListener(new ReplicateMetricsReadListener<>());
        if (logKeys) {
            log.info("Adding key logger");
            step.addListener(new ReplicateWriteLogger<>(log, reader.getCodec()));
            step.addListener(new ReplicateReadLogger<>(log, reader.getCodec()));
        }
        step.addListener(new ReplicateMetricsWriteListener<>());
        return step;
    }

    protected RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> scanStep() {
        return replicateStep(SCAN_STEP, new RedisScanItemReader<>(CODEC, readOperation()));
    }

    protected RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> liveStep() {
        return replicateStep(LIVE_STEP, new RedisLiveItemReader<>(CODEC, readOperation()));
    }

    protected ItemWriter<KeyValue<byte[]>> targetWriter() {
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> redisWriter = writer(writeOperation());
        configureTarget(redisWriter);
        ItemWriter<KeyValue<byte[]>> writer = RiotUtils.writer(processor(), redisWriter);
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
        return struct;
    }

    public void setStruct(boolean struct) {
        this.struct = struct;
    }

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        metricsArgs.configureMetrics();
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

    public MetricsArgs getMetricsArgs() {
        return metricsArgs;
    }

    public void setMetricsArgs(MetricsArgs metricsArgs) {
        this.metricsArgs = metricsArgs;
    }

    public boolean isRemoveSourceKeys() {
        return removeSourceKeys;
    }

    public void setRemoveSourceKeys(boolean removeSourceKeys) {
        this.removeSourceKeys = removeSourceKeys;
    }

}
