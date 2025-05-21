package com.redis.riot.core.job;

import com.redis.riot.core.NoopItemWriter;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.ThrottledItemWriter;
import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.RedisOOMException;
import com.redis.spring.batch.step.FlushingStepFactoryBean;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisLoadingException;
import org.springframework.batch.core.StepListener;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;

import java.text.ParseException;
import java.time.Duration;
import java.util.*;

public class StepFactoryBean<T, S> extends FlushingStepFactoryBean<T, S> {

    private static final int NAME_MAX_LENGTH = 80;

    public static final int DEFAULT_THREADS = 1;

    private final Set<Class<? extends Throwable>> skip = defaultSkippableExceptions();

    private final Set<Class<? extends Throwable>> noSkip = new HashSet<>();

    private final Set<Class<? extends Throwable>> retry = defaultRetriableExceptions();

    private final Set<Class<? extends Throwable>> noRetry = defaultNonRetriableExceptions();

    private int threads = DEFAULT_THREADS;

    private boolean dryRun;

    private Duration sleep;

    public static Set<Class<? extends Throwable>> defaultSkippableExceptions() {
        return BatchUtils.asSet(ParseException.class, org.springframework.batch.item.ParseException.class);
    }

    public static Set<Class<? extends Throwable>> defaultRetriableExceptions() {
        return BatchUtils.asSet(RedisCommandTimeoutException.class, RedisLoadingException.class, RedisBusyException.class,
                RedisOOMException.class);
    }

    public static Set<Class<? extends Throwable>> defaultNonRetriableExceptions() {
        Set<Class<? extends Throwable>> exceptions = BatchUtils.asSet(RedisCommandExecutionException.class);
        exceptions.addAll(defaultSkippableExceptions());
        return exceptions;
    }

    private ItemWriter<? super S> sleep(ItemWriter<? super S> writer) {
        if (sleep == null || sleep.isNegative() || sleep.isZero()) {
            return writer;
        }
        return new ThrottledItemWriter<>(writer, sleep);

    }

    private ItemWriter<? super S> dryRun(ItemWriter<? super S> writer) {
        if (dryRun) {
            return new NoopItemWriter<>();
        }
        return writer;
    }

    @Override
    protected void applyConfiguration(SimpleStepBuilder<T, S> builder) {
        if (getName().length() > NAME_MAX_LENGTH) {
            setBeanName(getName().substring(0, 69) + "…" + getName().substring(getName().length() - 10));
        }
        setSkippableExceptionClasses(skippableExceptions());
        setRetryableExceptionClasses(retryableExceptions());
        // do this first because we may override reader and writer
        super.applyConfiguration(builder);
        if (threads > 1) {
            builder.taskExecutor(RiotUtils.threadPoolTaskExecutor(threads));
            builder.throttleLimit(threads);
            builder.reader(synchronizedItemReader());
        }
        builder.writer(sleep(dryRun(getItemWriter())));
    }

    @SuppressWarnings("unchecked")
    protected ItemReader<? extends T> synchronizedItemReader() {
        ItemReader<? extends T> reader = getItemReader();
        if (reader instanceof PollableItemReader) {
            return reader;
        }
        if (reader instanceof ItemStreamReader) {
            SynchronizedItemStreamReader<T> synchronizedReader = new SynchronizedItemStreamReader<>();
            synchronizedReader.setDelegate((ItemStreamReader<T>) reader);
            return synchronizedReader;
        }
        return new SynchronizedItemReader<>(reader);
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

    public void addListener(StepListener listener) {
        StepListener[] listeners = Arrays.copyOf(getListeners(), getListeners().length + 1);
        listeners[listeners.length - 1] = listener;
        setListeners(listeners);
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

    @Override
    public ItemReader<? extends T> getItemReader() {
        return super.getItemReader();
    }

    @Override
    public ItemWriter<? super S> getItemWriter() {
        return super.getItemWriter();
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

    public Duration getSleep() {
        return sleep;
    }

    public void setSleep(Duration sleep) {
        this.sleep = sleep;
    }

}
