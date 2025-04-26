package com.redis.riot;

import java.time.Duration;
import java.util.stream.Collectors;

import com.redis.riot.core.CompareStepListener;
import com.redis.riot.core.RiotStep;
import com.redis.riot.core.RiotUtils;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.redis.riot.core.function.StringKeyValue;
import com.redis.riot.core.function.ToStringKeyValue;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.DefaultKeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;
import com.redis.spring.batch.item.redis.reader.KeyComparisonStat;
import com.redis.spring.batch.item.redis.reader.KeyComparisonStatsWriter;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

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

    protected abstract boolean isStruct();

    protected ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>> processor() {
        if (isIgnoreStreamMessageId()) {
            Assert.isTrue(isStruct(), "--no-stream-id can only be used with --struct");
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
        return CompareStepListener.statsByStatus(stats).stream()
                .map(e -> String.format("%s %d", e.getKey(),
                        e.getValue().stream().collect(Collectors.summingLong(KeyComparisonStat::getCount))))
                .collect(Collectors.joining(" | "));
    }

    protected RiotStep<KeyComparison<byte[]>, KeyComparison<byte[]>> compareStep() {
        RedisScanItemReader<byte[], byte[]> sourceReader = configureSource(compareReader());
        RedisScanItemReader<byte[], byte[]> targetReader = configureTarget(compareReader());
        KeyComparisonItemReader<byte[], byte[]> reader = new KeyComparisonItemReader<>(sourceReader, targetReader);
        reader.setComparator(keyComparator());
        reader.setProcessor(RiotUtils.processor(keyValueFilter(), processor()));
        KeyComparisonStatsWriter<byte[]> stats = new KeyComparisonStatsWriter<>();
        RiotStep<KeyComparison<byte[]>, KeyComparison<byte[]>> step = new RiotStep<>(COMPARE_STEP_NAME, reader, stats);
        if (showDiffs) {
            log.info("Adding key diff logger");
            step.addWriteListener(new CompareLoggingWriteListener<>(CODEC));
        }
        step.taskName(COMPARE_TASK_NAME);
        step.extraMessage(() -> compareMessage(stats));
        step.maxItemCount(BatchUtils.scanSizeEstimator(sourceReader));
        step.addExecutionListener(new CompareStepListener(stats));
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

}
