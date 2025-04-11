package com.redis.riot.core;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;

import lombok.ToString;

@ToString
public class RiotStep<I, O> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String name;

    private final ItemReader<I> reader;

    private final ItemWriter<O> writer;

    private JobRepository jobRepository;

    private PlatformTransactionManager transactionManager;

    private ItemProcessor<I, O> processor;

    private Set<StepExecutionListener> executionListeners = new LinkedHashSet<>();

    private Set<ItemReadListener<I>> readListeners = new LinkedHashSet<>();

    private Set<ItemWriteListener<O>> writeListeners = new LinkedHashSet<>();

    private Duration flushInterval;

    private Duration idleTimeout;

    private Collection<Class<? extends Throwable>> skip = new HashSet<>();

    private Collection<Class<? extends Throwable>> noSkip = new HashSet<>();

    private Collection<Class<? extends Throwable>> retry = new HashSet<>();

    private Collection<Class<? extends Throwable>> noRetry = new HashSet<>();

    private StepArgs stepArgs;

    private LongSupplier maxItemCount;

    private String taskName;

    private Supplier<String> extraMessage;

    public RiotStep(ItemReader<I> reader, ItemWriter<O> writer) {
        this.reader = reader;
        this.writer = writer;
    }

    public SimpleStepBuilder<I, O> build() {
        log.info("Creating step {} with {}", name, stepArgs);
        String stepName = stepName();
        SimpleStepBuilder<I, O> step = new StepBuilder(stepName, jobRepository).<I, O> chunk(stepArgs.getChunkSize(),
                transactionManager);
        step.reader(reader());
        step.writer(writer());
        step.processor(processor);
        step.taskExecutor(taskExecutor());
        step.throttleLimit(stepArgs.getThreads());
        executionListeners.forEach(step::listener);
        writeListeners.forEach(step::listener);
        if (reader instanceof PollableItemReader) {
            log.info("Creating flushing step with flush interval {} and idle timeout {}", flushInterval, idleTimeout);
            FlushingStepBuilder<I, O> flushingStepBuilder = new FlushingStepBuilder<>(step);
            flushingStepBuilder.flushInterval(flushInterval);
            flushingStepBuilder.idleTimeout(idleTimeout);
            return build(flushingStepBuilder);
        }
        return build(step);
    }

    private TaskExecutor taskExecutor() {
        if (stepArgs.getThreads() == 1) {
            return new SyncTaskExecutor();
        }
        log.info("Creating thread-pool task executor of size {}", stepArgs.getThreads());
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(stepArgs.getThreads());
        taskExecutor.setCorePoolSize(stepArgs.getThreads());
        taskExecutor.initialize();
        return taskExecutor;
    }

    private ItemReader<I> reader() {
        if (stepArgs.getThreads() == 1 || reader instanceof PollableItemReader) {
            return reader;
        }
        log.info("Synchronizing reader in step {}", name);
        if (reader instanceof ItemStreamReader) {
            SynchronizedItemStreamReader<I> synchronizedReader = new SynchronizedItemStreamReader<>();
            synchronizedReader.setDelegate((ItemStreamReader<I>) reader);
            return synchronizedReader;
        }
        return new SynchronizedItemReader<>(reader);
    }

    private ItemWriter<O> writer() {
        if (stepArgs.getSleep() == null) {
            return checkedWriter();
        }
        log.info("Throttling writer with sleep={}", stepArgs.getSleep());
        return new ThrottledItemWriter<>(checkedWriter(), stepArgs.getSleep().getValue());
    }

    private ItemWriter<O> checkedWriter() {
        if (stepArgs.isDryRun()) {
            log.info("Using no-op writer");
            return new NoopItemWriter<>();
        }
        return writer;
    }

    private SimpleStepBuilder<I, O> build(SimpleStepBuilder<I, O> builder) {
        if (stepArgs.getRetryPolicy() == RetryPolicy.NEVER && stepArgs.getSkipPolicy() == SkipPolicy.NEVER) {
            log.info("Skipping fault-tolerance for step {}", name);
            return builder;
        }
        log.info("Adding fault-tolerance to step {}", name);
        FaultTolerantStepBuilder<I, O> ftStep = builder.faultTolerant();
        skip.forEach(ftStep::skip);
        noSkip.forEach(ftStep::noSkip);
        retry.forEach(ftStep::retry);
        noRetry.forEach(ftStep::noRetry);
        ftStep.retryLimit(stepArgs.getRetryLimit());
        ftStep.retryPolicy(retryPolicy());
        ftStep.skipLimit(stepArgs.getSkipLimit());
        ftStep.skipPolicy(skipPolicy());
        return ftStep;
    }

    private org.springframework.retry.RetryPolicy retryPolicy() {
        switch (stepArgs.getRetryPolicy()) {
            case ALWAYS:
                return new AlwaysRetryPolicy();
            case NEVER:
                return new NeverRetryPolicy();
            default:
                return null;
        }
    }

    private org.springframework.batch.core.step.skip.SkipPolicy skipPolicy() {
        switch (stepArgs.getSkipPolicy()) {
            case ALWAYS:
                return new AlwaysSkipItemSkipPolicy();
            case NEVER:
                return new NeverSkipItemSkipPolicy();
            default:
                return null;
        }
    }

    private String stepName() {
        if (name.length() > 80) {
            return name.substring(0, 69) + "…" + name.substring(name.length() - 10);
        }
        return name;
    }

    public RiotStep<I, O> addReadListener(ItemReadListener<I> listener) {
        this.readListeners.add(listener);
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

    public void setName(String name) {
        this.name = name;
    }

    public JobRepository getJobRepository() {
        return jobRepository;
    }

    public void setJobRepository(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public ItemProcessor<I, O> getProcessor() {
        return processor;
    }

    public void setProcessor(ItemProcessor<I, O> processor) {
        this.processor = processor;
    }

    public Set<StepExecutionListener> getExecutionListeners() {
        return executionListeners;
    }

    public void setExecutionListeners(Set<StepExecutionListener> executionListeners) {
        this.executionListeners = executionListeners;
    }

    public Set<ItemReadListener<I>> getReadListeners() {
        return readListeners;
    }

    public void setReadListeners(Set<ItemReadListener<I>> readListeners) {
        this.readListeners = readListeners;
    }

    public Set<ItemWriteListener<O>> getWriteListeners() {
        return writeListeners;
    }

    public void setWriteListeners(Set<ItemWriteListener<O>> writeListeners) {
        this.writeListeners = writeListeners;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(Duration flushInterval) {
        this.flushInterval = flushInterval;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public Collection<Class<? extends Throwable>> getSkip() {
        return skip;
    }

    public void setSkip(Collection<Class<? extends Throwable>> skip) {
        this.skip = skip;
    }

    public Collection<Class<? extends Throwable>> getNoSkip() {
        return noSkip;
    }

    public void setNoSkip(Collection<Class<? extends Throwable>> noSkip) {
        this.noSkip = noSkip;
    }

    public Collection<Class<? extends Throwable>> getRetry() {
        return retry;
    }

    public void setRetry(Collection<Class<? extends Throwable>> retry) {
        this.retry = retry;
    }

    public Collection<Class<? extends Throwable>> getNoRetry() {
        return noRetry;
    }

    public void setNoRetry(Collection<Class<? extends Throwable>> noRetry) {
        this.noRetry = noRetry;
    }

    public StepArgs getStepArgs() {
        return stepArgs;
    }

    public void setStepArgs(StepArgs stepArgs) {
        this.stepArgs = stepArgs;
    }

    public LongSupplier getMaxItemCount() {
        return maxItemCount;
    }

    public void setMaxItemCount(LongSupplier maxItemCount) {
        this.maxItemCount = maxItemCount;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public ItemReader<I> getReader() {
        return reader;
    }

    public ItemWriter<O> getWriter() {
        return writer;
    }

    public Supplier<String> getExtraMessage() {
        return extraMessage;
    }

    public void setExtraMessage(Supplier<String> extraMessage) {
        this.extraMessage = extraMessage;
    }

}
