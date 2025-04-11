package com.redis.riot;

import java.util.function.LongSupplier;

import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.redis.riot.core.RiotStep;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.RedisLiveItemReader;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import com.redis.spring.batch.item.redis.reader.RedisScanSizeEstimator;

import io.lettuce.core.codec.RedisCodec;

public class ExportFlow<K, V, T> {

    public enum ExecutionMode {
        SERIAL, PARALLEL
    }

    private static final String LIVE_TASK_NAME = "Listening";

    private static final String SCAN_TASK_NAME = "Scanning";

    private final RedisCodec<K, V> codec;

    private final ItemWriter<T> writer;

    private ExecutionMode executionMode;

    private RedisReaderMode readerMode;

    private RedisContext redisContext;

    private RedisReaderArgs readerArgs;

    private MemoryUsageArgs memoryUsageArgs;

    public ExportFlow(RedisCodec<K, V> codec, ItemWriter<T> writer) {
        this.codec = codec;
        this.writer = writer;
    }

    public Flow build() {
        switch (readerMode) {
            case BOTH:
                return combinedFlow();
            case LIVE:
                return liveFlow();
            default:
                return scanFlow();
        }
    }

    private Flow combinedFlow() {
        if (executionMode == ExecutionMode.SERIAL) {
            return flow("serialFlow").start(scanFlow()).next(liveFlow()).build();
        }
        return flow("parallelFlow").split(asyncTaskExecutor()).add(scanFlow(), liveFlow()).build();

    }

    private Flow scanFlow() {
        return flow("scanFlow").start(scanStep().build().build()).build();
    }

    private Flow liveFlow() {
        return flow("liveFlow").start(liveStep().build().build()).build();
    }

    private FlowBuilder<SimpleFlow> flow(String name) {
        return new FlowBuilder<>(name);
    }

    private TaskExecutor asyncTaskExecutor() {
        return new SimpleAsyncTaskExecutor("export");
    }

    private void configureSource(RedisItemReader<K, V> reader) {
        redisContext.configure(reader);
        readerArgs.configure(reader);
        reader.setMemoryUsage(memoryUsageArgs.memoryUsage());
    }

    private RiotStep<KeyValue<K>, T> scanStep() {
        RedisScanItemReader<K, V> reader = RedisItemReader.scanStruct(codec);
        configureSource(reader);
        RiotStep<KeyValue<K>, T> step = step(reader, SCAN_TASK_NAME);
        step.setMaxItemCount(scanSizeEstimator(reader));
        return step;
    }

    private RiotStep<KeyValue<K>, T> step(RedisItemReader<K, V> reader, String taskName) {
        RiotStep<KeyValue<K>, T> step = new RiotStep<>(reader, writer);
        step.setTaskName(taskName);
        return step;
    }

    private RiotStep<KeyValue<K>, T> liveStep() {
        RedisLiveItemReader<K, V> reader = RedisItemReader.liveStruct(codec);
        configureSource(reader);
        return step(reader, LIVE_TASK_NAME);
    }

    private LongSupplier scanSizeEstimator(RedisScanItemReader<K, V> reader) {
        return RedisScanSizeEstimator.from(reader.getClient(), reader.getKeyPattern(), reader.getKeyType());
    }

    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public void setExecutionMode(ExecutionMode executionMode) {
        this.executionMode = executionMode;
    }

    public RedisReaderMode getReaderMode() {
        return readerMode;
    }

    public void setReaderMode(RedisReaderMode readerMode) {
        this.readerMode = readerMode;
    }

    public RedisContext getRedisContext() {
        return redisContext;
    }

    public void setRedisContext(RedisContext redisContext) {
        this.redisContext = redisContext;
    }

    public RedisReaderArgs getReaderArgs() {
        return readerArgs;
    }

    public void setReaderArgs(RedisReaderArgs readerArgs) {
        this.readerArgs = readerArgs;
    }

    public MemoryUsageArgs getMemoryUsageArgs() {
        return memoryUsageArgs;
    }

    public void setMemoryUsageArgs(MemoryUsageArgs memoryUsageArgs) {
        this.memoryUsageArgs = memoryUsageArgs;
    }

}
