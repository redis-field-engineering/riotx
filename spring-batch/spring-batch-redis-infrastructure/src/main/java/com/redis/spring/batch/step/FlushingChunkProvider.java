package com.redis.spring.batch.step;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.observability.BatchMetrics;
import org.springframework.batch.core.step.item.SimpleChunkProvider;
import org.springframework.batch.core.step.item.SkipOverflowException;
import org.springframework.batch.core.step.skip.SkipListenerFailedException;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;

import com.redis.spring.batch.item.PollableItemReader;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer.Sample;

/**
 * Fault-tolerant implementation of the ChunkProvider interface, that allows for skipping or retry of items that cause
 * exceptions, as well as incomplete chunks when timeout is reached.
 */
public class FlushingChunkProvider<I> extends SimpleChunkProvider<I> {

    public static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofMillis(50);

    private final RepeatOperations repeatOperations;

    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

    private Duration idleTimeout; // no idle timeout by default

    private MeterRegistry meterRegistry = Metrics.globalRegistry;

    private long latest = System.currentTimeMillis();

    public FlushingChunkProvider(ItemReader<? extends I> itemReader, RepeatOperations repeatOperations) {
        super(itemReader, repeatOperations);
        Assert.isTrue(itemReader instanceof PollableItemReader, "Reader must extend PollableItemReader");
        this.repeatOperations = repeatOperations;
    }

    @Override
    public Chunk<I> provide(StepContribution contribution) {
        Chunk<I> inputs = new Chunk<>();
        long start = System.currentTimeMillis();
        repeatOperations.iterate(context -> {
            long idleMillis = System.currentTimeMillis() - latest;
            if (idleTimeout != null && idleMillis > idleTimeout.toMillis()) {
                inputs.setEnd();
                return RepeatStatus.FINISHED;
            }
            long remainingMillis = flushInterval.toMillis() - (System.currentTimeMillis() - start);
            if (remainingMillis < 0) {
                return RepeatStatus.FINISHED;
            }
            Sample sample = BatchMetrics.createTimerSample(this.meterRegistry);
            String status = BatchMetrics.STATUS_SUCCESS;
            I item;
            try {
                item = read(contribution, inputs, remainingMillis);
            } catch (SkipOverflowException e) {
                // read() tells us about an excess of skips by throwing an exception
                status = BatchMetrics.STATUS_FAILURE;
                return RepeatStatus.FINISHED;
            } finally {
                stopTimer(contribution.getStepExecution(), sample, status);
            }
            if (item == null) {
                return RepeatStatus.CONTINUABLE;
            }
            inputs.add(item);
            contribution.incrementReadCount();
            latest = System.currentTimeMillis();
            return RepeatStatus.CONTINUABLE;
        });
        return inputs;
    }

    private void stopTimer(StepExecution execution, Sample sample, String status) {
        String fullyQualifiedMetricName = BatchMetrics.METRICS_PREFIX + "item.read";
        sample.stop(BatchMetrics.createTimer(meterRegistry, "item.read", "Item reading duration",
                Tag.of(fullyQualifiedMetricName + ".job.name", execution.getJobExecution().getJobInstance().getJobName()),
                Tag.of(fullyQualifiedMetricName + ".step.name", execution.getStepName()),
                Tag.of(fullyQualifiedMetricName + ".status", status)));

    }

    protected I read(StepContribution contribution, Chunk<I> chunk, long timeout) throws Exception {
        return doRead(timeout);
    }

    @SuppressWarnings("unchecked")
    protected final I doRead(long timeout) throws Exception {
        try {
            getListener().beforeRead();
            I item = ((PollableItemReader<I>) itemReader).poll(timeout, TimeUnit.MILLISECONDS);
            if (item != null) {
                getListener().afterRead(item);
            }
            return item;
        } catch (Exception e) {
            getListener().onReadError(e);
            throw e;
        }
    }

    @Override
    public void postProcess(StepContribution contribution, Chunk<I> chunk) {
        for (Exception e : chunk.getErrors()) {
            try {
                getListener().onSkipInRead(e);
            } catch (RuntimeException ex) {
                throw new SkipListenerFailedException("Fatal exception in SkipListener.", ex, e);
            }
        }
    }

    @Override
    public void setMeterRegistry(MeterRegistry meterRegistry) {
        super.setMeterRegistry(meterRegistry);
        this.meterRegistry = meterRegistry;
    }

    public void setFlushInterval(Duration interval) {
        this.flushInterval = interval;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

}
