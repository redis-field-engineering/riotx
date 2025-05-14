package com.redis.riot;

import com.redis.riot.core.CompareStepListener;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.function.StringKeyValue;
import com.redis.riot.core.function.ToStringKeyValue;
import com.redis.riot.core.job.RiotStep;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractCompareCommand extends AbstractRedisTargetExport {

    protected static final String COMPARE_STEP_NAME = "compare";

    public static final Duration DEFAULT_TTL_TOLERANCE = DefaultKeyComparator.DEFAULT_TTL_TOLERANCE;

    public static final boolean DEFAULT_COMPARE_STREAM_MESSAGE_ID = true;

    protected static final RedisCodec<byte[], byte[]> CODEC = ByteArrayCodec.INSTANCE;

    private static final String COMPARE_TASK_NAME = "Comparing";

    @Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification. Disables progress reporting.")
    private boolean showDiffs;

    @Option(names = "--ttl-tolerance", description = "Max TTL delta to consider keys equal (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    @ArgGroup(exclusive = false)
    private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

    @ArgGroup(exclusive = false, heading = "Processor options%n")
    private KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        if (step.getName().equals(COMPARE_STEP_NAME)) {
            return COMPARE_TASK_NAME;
        }
        return null;
    }

    protected abstract boolean isStruct();

    protected ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>> processor() {
        if (isIgnoreStreamMessageId()) {
            Assert.isTrue(isStruct(), "Dropping stream message ID is only possible in STRUCT mode");
        }
        StandardEvaluationContext evaluationContext = evaluationContext();
        log.info("Creating processor with {}", processorArgs);
        ItemProcessor<KeyValue<String>, KeyValue<String>> processor = processorArgs.processor(evaluationContext);
        if (processor == null) {
            return null;
        }
        ToStringKeyValue<byte[]> code = new ToStringKeyValue<>(CODEC);
        StringKeyValue<byte[]> decode = new StringKeyValue<>(CODEC);
        return RiotUtils.processor(new FunctionItemProcessor<>(code), processor, new FunctionItemProcessor<>(decode));
    }

    private StandardEvaluationContext evaluationContext() {
        log.info("Creating SpEL evaluation context with {}", evaluationContextArgs);
        StandardEvaluationContext evaluationContext = evaluationContextArgs.evaluationContext();
        configure(evaluationContext);
        return evaluationContext;
    }

    private <K> String compareMessage(KeyComparisonStatsWriter<K> stats) {
        return CompareStepListener.statsByStatus(stats).stream().map(this::formatStatus).collect(Collectors.joining(" | "));
    }

    private String formatStatus(Map.Entry<KeyComparison.Status, List<KeyComparisonStat>> e) {
        return String.format("%s %d", e.getKey(), e.getValue().stream().mapToLong(KeyComparisonStat::getCount).sum());
    }

    protected RiotStep<KeyComparison<byte[]>, KeyComparison<byte[]>> compareStep() {
        RedisScanItemReader<byte[], byte[]> sourceReader = compareReader();
        configureSource(sourceReader);
        RedisScanItemReader<byte[], byte[]> targetReader = compareReader();
        configureTarget(targetReader);
        KeyComparisonItemReader<byte[], byte[]> reader = new KeyComparisonItemReader<>(sourceReader, targetReader);
        reader.setBatchSize(getStepArgs().getChunkSize());
        reader.setComparator(keyComparator());
        reader.setProcessor(RiotUtils.processor(keyValueFilter(), processor()));
        KeyComparisonStatsWriter<byte[]> writer = new KeyComparisonStatsWriter<>();
        RiotStep<KeyComparison<byte[]>, KeyComparison<byte[]>> step = step(COMPARE_STEP_NAME, reader, writer);
        if (showDiffs) {
            log.info("Adding key diff logger");
            step.addWriteListener(new CompareLoggingWriteListener<>(CODEC));
        }
        step.addExecutionListener(new CompareStepListener(writer));
        return step;
    }

    private RedisScanItemReader<byte[], byte[]> compareReader() {
        return new RedisScanItemReader<>(CODEC, compareReadOperation());
    }

    private KeyValueRead<byte[], byte[]> compareReadOperation() {
        if (isQuickCompare()) {
            log.info("Creating Redis quick compare reader");
            return KeyValueRead.type(CODEC);
        }
        log.info("Creating Redis full compare reader");
        return KeyValueRead.struct(CODEC);
    }

    protected abstract boolean isQuickCompare();

    private KeyComparator<byte[]> keyComparator() {
        boolean ignoreStreamId = isIgnoreStreamMessageId();
        log.info("Creating KeyComparator with ttlTolerance={} ignoreStreamMessageId={}", ttlTolerance, ignoreStreamId);
        DefaultKeyComparator<byte[], byte[]> comparator = new DefaultKeyComparator<>(CODEC);
        comparator.setIgnoreStreamMessageId(ignoreStreamId);
        comparator.setTtlTolerance(ttlTolerance);
        return comparator;
    }

    protected boolean isIgnoreStreamMessageId() {
        return processorArgs.isNoStreamIds();
    }

    public boolean isShowDiffs() {
        return showDiffs;
    }

    public void setShowDiffs(boolean showDiffs) {
        this.showDiffs = showDiffs;
    }

    public Duration getTtlTolerance() {
        return ttlTolerance;
    }

    public void setTtlTolerance(Duration tolerance) {
        this.ttlTolerance = tolerance;
    }

    public KeyValueProcessorArgs getProcessorArgs() {
        return processorArgs;
    }

    public void setProcessorArgs(KeyValueProcessorArgs args) {
        this.processorArgs = args;
    }

    public EvaluationContextArgs getEvaluationContextArgs() {
        return evaluationContextArgs;
    }

    public void setEvaluationContextArgs(EvaluationContextArgs evaluationContextArgs) {
        this.evaluationContextArgs = evaluationContextArgs;
    }

}
