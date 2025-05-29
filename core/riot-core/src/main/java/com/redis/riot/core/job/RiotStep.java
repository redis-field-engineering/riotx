package com.redis.riot.core.job;

import com.redis.riot.core.BackpressureException;
import com.redis.riot.core.NoopItemWriter;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.ThrottledItemWriter;
import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.RedisOOMException;
import com.redis.spring.batch.step.FlushingFaultTolerantStepFactoryBean;
import com.redis.spring.batch.step.FlushingStepFactoryBean;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisLoadingException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.factory.FaultTolerantStepFactoryBean;
import org.springframework.batch.core.step.factory.SimpleStepFactoryBean;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.transaction.PlatformTransactionManager;

import java.text.ParseException;
import java.time.Duration;
import java.util.*;

public class RiotStep<T, S> implements FactoryBean<Step> {

    public static final int DEFAULT_THREADS = 1;

    public static final int DEFAULT_COMMIT_INTERVAL = 50;

    private final Set<Class<? extends Throwable>> skip = defaultSkippableExceptions();

    private final Set<Class<? extends Throwable>> noSkip = new HashSet<>();

    private final Set<Class<? extends Throwable>> retry = defaultRetriableExceptions();

    private final Set<Class<? extends Throwable>> noRetry = defaultNonRetriableExceptions();

    private final Set<StepListener> listeners = new LinkedHashSet<>();

    private String name;

    private ItemReader<? extends T> itemReader;

    private ItemProcessor<? super T, ? extends S> itemProcessor;

    private ItemWriter<? super S> itemWriter;

    private PlatformTransactionManager transactionManager;

    protected JobRepository jobRepository;

    private int commitInterval = DEFAULT_COMMIT_INTERVAL;

    private int threads = DEFAULT_THREADS;

    private boolean dryRun;

    private int writesPerSecond;

    private Duration flushInterval = FlushingStepFactoryBean.DEFAULT_FLUSH_INTERVAL;

    private Duration idleTimeout = FlushingStepFactoryBean.DEFAULT_IDLE_TIMEOUT;

    private int retryLimit;

    private int skipLimit;

    private BackOffPolicy backOffPolicy;

    @Override
    public Step getObject() throws Exception {
        SimpleStepFactoryBean<T, S> factory = factoryBean();
        factory.setBeanName(RiotUtils.normalizeName(name));
        factory.setCommitInterval(commitInterval);
        factory.setItemProcessor(itemProcessor);
        factory.setItemReader(itemReader);
        factory.setItemWriter(itemWriter());
        factory.setJobRepository(jobRepository);
        factory.setListeners(listeners.toArray(new StepListener[0]));
        factory.setTransactionManager(transactionManager);
        if (threads > 1) {
            factory.setTaskExecutor(RiotUtils.threadPoolTaskExecutor(threads));
            factory.setThrottleLimit(threads);
            factory.setItemReader(synchronizedItemReader());
        }
        return factory.getObject();
    }

    @Override
    public Class<TaskletStep> getObjectType() {
        return TaskletStep.class;
    }

    protected boolean isFaultTolerant() {
        return retryLimit != 0 || skipLimit != 0;
    }

    private SimpleStepFactoryBean<T, S> factoryBean() {
        if (itemReader instanceof PollableItemReader) {
            if (isFaultTolerant()) {
                FlushingFaultTolerantStepFactoryBean<T, S> factoryBean = new FlushingFaultTolerantStepFactoryBean<>();
                factoryBean.setFlushInterval(flushInterval);
                factoryBean.setIdleTimeout(idleTimeout);
                configureFaultTolerantStepFactoryBean(factoryBean);
                return factoryBean;
            }
            FlushingStepFactoryBean<T, S> factoryBean = new FlushingStepFactoryBean<>();
            factoryBean.setFlushInterval(flushInterval);
            factoryBean.setIdleTimeout(idleTimeout);
            return factoryBean;
        }
        if (isFaultTolerant()) {
            FaultTolerantStepFactoryBean<T, S> factoryBean = new FaultTolerantStepFactoryBean<>();
            configureFaultTolerantStepFactoryBean(factoryBean);
            return factoryBean;
        }
        return new SimpleStepFactoryBean<>();
    }

    private void configureFaultTolerantStepFactoryBean(FaultTolerantStepFactoryBean<T, S> factoryBean) {
        if (retryLimit > 0) {
            factoryBean.setRetryLimit(retryLimit);
        } else {
            factoryBean.setRetryPolicy(retryPolicy());
        }
        if (skipLimit > 0) {
            factoryBean.setSkipLimit(skipLimit);
        } else {
            factoryBean.setSkipPolicy(skipPolicy());
        }
        factoryBean.setRetryableExceptionClasses(retryableExceptions());
        factoryBean.setSkippableExceptionClasses(skippableExceptions());
        factoryBean.setBackOffPolicy(backOffPolicy);
    }

    private SkipPolicy skipPolicy() {
        if (skipLimit < 0) {
            return new AlwaysSkipItemSkipPolicy();
        }
        return new NeverSkipItemSkipPolicy();
    }

    private RetryPolicy retryPolicy() {
        if (retryLimit < 0) {
            return new AlwaysRetryPolicy();
        }
        return new NeverRetryPolicy();
    }

    public static Set<Class<? extends Throwable>> defaultSkippableExceptions() {
        return BatchUtils.asSet(ParseException.class, org.springframework.batch.item.ParseException.class);
    }

    public static Set<Class<? extends Throwable>> defaultRetriableExceptions() {
        return BatchUtils.asSet(RedisCommandTimeoutException.class, RedisLoadingException.class, RedisBusyException.class,
                RedisOOMException.class, BackpressureException.class);
    }

    public static Set<Class<? extends Throwable>> defaultNonRetriableExceptions() {
        Set<Class<? extends Throwable>> exceptions = BatchUtils.asSet(RedisCommandExecutionException.class);
        exceptions.addAll(defaultSkippableExceptions());
        return exceptions;
    }

    private ItemWriter<? super S> rateLimit(ItemWriter<? super S> writer) {
        if (writesPerSecond > 0) {
            return new ThrottledItemWriter<>(writer, writesPerSecond);
        }
        return writer;
    }

    private ItemWriter<? super S> dryRun(ItemWriter<? super S> writer) {
        if (dryRun) {
            return new NoopItemWriter<>();
        }
        return writer;
    }

    protected ItemReader<? extends T> synchronizedItemReader() {
        if (itemReader instanceof PollableItemReader) {
            return itemReader;
        }
        if (itemReader instanceof ItemStreamReader) {
            SynchronizedItemStreamReader<T> synchronizedReader = new SynchronizedItemStreamReader<>();
            synchronizedReader.setDelegate((ItemStreamReader<T>) itemReader);
            return synchronizedReader;
        }
        return new SynchronizedItemReader<>(itemReader);
    }

    protected ItemWriter<? super S> itemWriter() {
        return rateLimit(dryRun(itemWriter));
    }

    private Map<Class<? extends Throwable>, Boolean> retryableExceptions() {
        Map<Class<? extends Throwable>, Boolean> exceptionClasses = new LinkedHashMap<>();
        noRetry.forEach(e -> exceptionClasses.put(e, false));
        retry.forEach(e -> exceptionClasses.put(e, true));
        return exceptionClasses;
    }

    private Map<Class<? extends Throwable>, Boolean> skippableExceptions() {
        Map<Class<? extends Throwable>, Boolean> exceptionClasses = new LinkedHashMap<>();
        noSkip.forEach(e -> exceptionClasses.put(e, false));
        skip.forEach(e -> exceptionClasses.put(e, true));
        return exceptionClasses;
    }

    public void skip(Class<? extends Throwable> exception) {
        skip.add(exception);
    }

    public void retry(Class<? extends Throwable> exception) {
        retry.add(exception);
    }

    public void noSkip(Class<? extends Throwable> exception) {
        noSkip.add(exception);
    }

    public void noRetry(Class<? extends Throwable> exception) {
        noRetry.add(exception);
    }

    public void addListener(StepListener listener) {
        listeners.add(listener);
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public int getWritesPerSecond() {
        return writesPerSecond;
    }

    public void setWritesPerSecond(int writesPerSecond) {
        this.writesPerSecond = writesPerSecond;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ItemReader<? extends T> getItemReader() {
        return itemReader;
    }

    public void setItemReader(ItemReader<? extends T> itemReader) {
        this.itemReader = itemReader;
    }

    public ItemProcessor<? super T, ? extends S> getItemProcessor() {
        return itemProcessor;
    }

    public void setItemProcessor(ItemProcessor<? super T, ? extends S> itemProcessor) {
        this.itemProcessor = itemProcessor;
    }

    public ItemWriter<? super S> getItemWriter() {
        return itemWriter;
    }

    public void setItemWriter(ItemWriter<? super S> itemWriter) {
        this.itemWriter = itemWriter;
    }

    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public JobRepository getJobRepository() {
        return jobRepository;
    }

    public void setJobRepository(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    public int getCommitInterval() {
        return commitInterval;
    }

    public void setCommitInterval(int commitInterval) {
        this.commitInterval = commitInterval;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public void setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
    }

    public int getSkipLimit() {
        return skipLimit;
    }

    public void setSkipLimit(int skipLimit) {
        this.skipLimit = skipLimit;
    }

    public BackOffPolicy getBackOffPolicy() {
        return backOffPolicy;
    }

    public void setBackOffPolicy(BackOffPolicy backOffPolicy) {
        this.backOffPolicy = backOffPolicy;
    }

}
