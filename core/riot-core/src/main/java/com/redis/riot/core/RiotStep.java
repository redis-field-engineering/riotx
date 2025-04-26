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
import org.springframework.batch.core.ItemProcessListener;
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

    private Set<ItemProcessListener<I, O>> processListeners = new LinkedHashSet<>();

    private Set<ItemWriteListener<O>> writeListeners = new LinkedHashSet<>();

    private Duration flushInterval;

    private Duration idleTimeout;

    private Collection<Class<? extends Throwable>> skip = new HashSet<>();

    private Collection<Class<? extends Throwable>> noSkip = new HashSet<>();

    private Collection<Class<? extends Throwable>> retry = new HashSet<>();

    private Collection<Class<? extends Throwable>> noRetry = new HashSet<>();

    private StepOptions options;

    private LongSupplier maxItemCount;

    private String taskName;

    private Supplier<String> extraMessage;

    public RiotStep(String name, ItemReader<I> reader, ItemWriter<O> writer) {
        this.name = name;
        this.reader = reader;
        this.writer = writer;
    }

    @SuppressWarnings("removal")
    public SimpleStepBuilder<I, O> build() {
        log.info("Creating step {} with {}", name, options);
        String stepName = stepName();
        SimpleStepBuilder<I, O> step = new StepBuilder(stepName, jobRepository).<I, O> chunk(options.getChunkSize(),
                transactionManager);
        step.reader(reader());
        step.writer(writer());
        step.processor(processor);
        step.taskExecutor(taskExecutor());
        step.throttleLimit(options.getThreads());
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
        if (options.getThreads() == 1) {
            return new SyncTaskExecutor();
        }
        log.info("Creating thread-pool task executor of size {}", options.getThreads());
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(options.getThreads());
        taskExecutor.setCorePoolSize(options.getThreads());
        taskExecutor.initialize();
        return taskExecutor;
    }

    private ItemReader<I> reader() {
        if (options.getThreads() == 1 || reader instanceof PollableItemReader) {
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
        if (options.getSleep() == null) {
            return checkedWriter();
        }
        log.info("Throttling writer with sleep={}", options.getSleep());
        return new ThrottledItemWriter<>(checkedWriter(), options.getSleep());
    }

    private ItemWriter<O> checkedWriter() {
        if (options.isDryRun()) {
            log.info("Using no-op writer");
            return new NoopItemWriter<>();
        }
        return writer;
    }

    private SimpleStepBuilder<I, O> build(SimpleStepBuilder<I, O> builder) {
        if (options.getRetryPolicy() == RetryPolicy.NEVER && options.getSkipPolicy() == SkipPolicy.NEVER) {
            log.info("Skipping fault-tolerance for step {}", name);
            return builder;
        }
        log.info("Adding fault-tolerance to step {}", name);
        FaultTolerantStepBuilder<I, O> ftStep = builder.faultTolerant();
        skip.forEach(ftStep::skip);
        noSkip.forEach(ftStep::noSkip);
        retry.forEach(ftStep::retry);
        noRetry.forEach(ftStep::noRetry);
        ftStep.retryLimit(options.getRetryLimit());
        ftStep.retryPolicy(retryPolicy());
        ftStep.skipLimit(options.getSkipLimit());
        ftStep.skipPolicy(skipPolicy());
        return ftStep;
    }

    private org.springframework.retry.RetryPolicy retryPolicy() {
        switch (options.getRetryPolicy()) {
            case ALWAYS:
                return new AlwaysRetryPolicy();
            case NEVER:
                return new NeverRetryPolicy();
            default:
                return null;
        }
    }

    private org.springframework.batch.core.step.skip.SkipPolicy skipPolicy() {
        switch (options.getSkipPolicy()) {
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

    public JobRepository getJobRepository() {
        return jobRepository;
    }

    public RiotStep<I, O> jobRepository(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
        return this;
    }

    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    public RiotStep<I, O> transactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
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

    public StepOptions getOptions() {
        return options;
    }

    public RiotStep<I, O> stepOptions(StepOptions options) {
        this.options = options;
        return this;
    }

    public LongSupplier getMaxItemCount() {
        return maxItemCount;
    }

    public RiotStep<I, O> maxItemCount(LongSupplier maxItemCount) {
        this.maxItemCount = maxItemCount;
        return this;
    }

    public String getTaskName() {
        return taskName;
    }

    public RiotStep<I, O> taskName(String taskName) {
        this.taskName = taskName;
        return this;
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

    public RiotStep<I, O> extraMessage(Supplier<String> messageSupplier) {
        this.extraMessage = messageSupplier;
        return this;
    }

}
