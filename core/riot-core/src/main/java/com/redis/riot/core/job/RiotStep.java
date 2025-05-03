package com.redis.riot.core.job;

import com.redis.spring.batch.step.FlushingChunkProvider;
import lombok.ToString;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

@ToString
public class RiotStep<I, O> {

    public static final int DEFAULT_CHUNK_SIZE = 50;

    public static final int DEFAULT_THREADS = 1;

    public static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicy.NEVER;

    public static final SkipPolicy DEFAULT_SKIP_POLICY = SkipPolicy.NEVER;

    public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;

    private String name;

    private final ItemReader<I> reader;

    private final ItemWriter<O> writer;

    private ItemProcessor<I, O> processor;

    private final Set<StepExecutionListener> executionListeners = new LinkedHashSet<>();

    private final Set<ItemReadListener<I>> readListeners = new LinkedHashSet<>();

    private final Set<ItemProcessListener<I, O>> processListeners = new LinkedHashSet<>();

    private final Set<ItemWriteListener<O>> writeListeners = new LinkedHashSet<>();

    private Duration flushInterval = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

    private Duration idleTimeout = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;

    private final Set<Class<? extends Throwable>> skip = new HashSet<>();

    private final Set<Class<? extends Throwable>> noSkip = new HashSet<>();

    private final Set<Class<? extends Throwable>> retry = new HashSet<>();

    private final Set<Class<? extends Throwable>> noRetry = new HashSet<>();

    private Duration sleep;

    private int threads = DEFAULT_THREADS;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private boolean dryRun;

    private SkipPolicy skipPolicy = DEFAULT_SKIP_POLICY;

    private int skipLimit;

    private RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;

    private int retryLimit = DEFAULT_RETRY_LIMIT;

    public RiotStep(String name, ItemReader<I> reader, ItemWriter<O> writer) {
        this.name = name;
        this.reader = reader;
        this.writer = writer;
    }

    public RiotStep<I, O> addReadListener(ItemReadListener<I> listener) {
        this.readListeners.add(listener);
        return this;
    }

    public RiotStep<I, O> addProcessListener(ItemProcessListener<I, O> listener) {
        this.processListeners.add(listener);
        return this;
    }

    public RiotStep<I, O> addWriteListener(ItemWriteListener<O> listener) {
        writeListeners.add(listener);
        return this;
    }

    public RiotStep<I, O> addExecutionListener(StepExecutionListener listener) {
        executionListeners.add(listener);
        return this;
    }

    public RiotStep<I, O> skip(Class<? extends Throwable> exception) {
        skip.add(exception);
        return this;
    }

    public RiotStep<I, O> retry(Class<? extends Throwable> exception) {
        retry.add(exception);
        return this;
    }

    public RiotStep<I, O> noSkip(Class<? extends Throwable> exception) {
        noSkip.add(exception);
        return this;
    }

    public RiotStep<I, O> noRetry(Class<? extends Throwable> exception) {
        noRetry.add(exception);
        return this;
    }

    public String getName() {
        return name;
    }

    public RiotStep<I, O> name(String name) {
        this.name = name;
        return this;
    }

    public ItemProcessor<I, O> getProcessor() {
        return processor;
    }

    public RiotStep<I, O> processor(ItemProcessor<I, O> processor) {
        this.processor = processor;
        return this;
    }

    public Set<StepExecutionListener> getExecutionListeners() {
        return executionListeners;
    }

    public Set<ItemProcessListener<I, O>> getProcessListeners() {
        return processListeners;
    }

    public Set<ItemReadListener<I>> getReadListeners() {
        return readListeners;
    }

    public Set<ItemWriteListener<O>> getWriteListeners() {
        return writeListeners;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public RiotStep<I, O> flushInterval(Duration flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public RiotStep<I, O> idleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    }

    public Collection<Class<? extends Throwable>> getSkip() {
        return skip;
    }

    public Collection<Class<? extends Throwable>> getNoSkip() {
        return noSkip;
    }

    public Collection<Class<? extends Throwable>> getRetry() {
        return retry;
    }

    public Collection<Class<? extends Throwable>> getNoRetry() {
        return noRetry;
    }

    public ItemReader<I> getReader() {
        return reader;
    }

    public ItemWriter<O> getWriter() {
        return writer;
    }

    public Duration getSleep() {
        return sleep;
    }

    public void setSleep(Duration sleep) {
        this.sleep = sleep;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public int getSkipLimit() {
        return skipLimit;
    }

    public void setSkipLimit(int skipLimit) {
        this.skipLimit = skipLimit;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public void setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
    }

    public SkipPolicy getSkipPolicy() {
        return skipPolicy;
    }

    public void setSkipPolicy(SkipPolicy skipPolicy) {
        this.skipPolicy = skipPolicy;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

}
