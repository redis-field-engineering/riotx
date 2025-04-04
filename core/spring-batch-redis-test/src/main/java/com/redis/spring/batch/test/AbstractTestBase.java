package com.redis.spring.batch.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.testcontainers.lifecycle.Startable;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.JobUtils;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.Range;
import com.redis.spring.batch.item.redis.common.RedisOperation;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.ItemType;
import com.redis.spring.batch.item.redis.gen.StreamOptions;
import com.redis.spring.batch.item.redis.reader.StreamItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractTestBase {

    public static final int DEFAULT_CHUNK_SIZE = 50;

    public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(1000);

    public static final Duration DEFAULT_POLL_DELAY = Duration.ZERO;

    public static final Duration DEFAULT_AWAIT_POLL_INTERVAL = Duration.ofMillis(10);

    public static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(3);

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

    private Duration awaitPollDelay = DEFAULT_POLL_DELAY;

    private Duration awaitPollInterval = DEFAULT_AWAIT_POLL_INTERVAL;

    private Duration awaitTimeout = DEFAULT_AWAIT_TIMEOUT;

    protected RedisURI redisURI;

    protected AbstractRedisClient redisClient;

    protected StatefulRedisModulesConnection<String, String> redisConnection;

    protected RedisModulesCommands<String, String> redisCommands;

    protected RedisModulesAsyncCommands<String, String> redisAsyncCommands;

    protected JobRepository jobRepository;

    private PlatformTransactionManager transactionManager;

    private TaskExecutorJobLauncher jobLauncher;

    public static RedisURI redisURI(RedisServer server) {
        return RedisURI.create(server.getRedisURI());
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    @BeforeAll
    void setup() throws Exception {
        // Source Redis setup
        RedisServer redis = getRedisServer();
        if (redis instanceof Startable) {
            ((Startable) redis).start();
        }
        redisURI = redisURI(redis);
        redisClient = client(redis);
        redisConnection = BatchUtils.connection(redisClient);
        redisCommands = redisConnection.sync();
        redisAsyncCommands = redisConnection.async();
        log.info("Successfully set up Redis:\n{}", redisCommands.info());
        jobRepository = JobUtils.jobRepositoryFactoryBean(ClassUtils.getShortName(getClass())).getObject();
        Assert.notNull(jobRepository, "Job repository is null");
        transactionManager = JobUtils.resourcelessTransactionManager();
        jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setTaskExecutor(new SyncTaskExecutor());
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet();
    }

    @AfterAll
    void teardown() {
        if (redisConnection != null) {
            redisConnection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
            redisClient.getResources().shutdown();
        }
        RedisServer redis = getRedisServer();
        if (redis instanceof Startable) {
            ((Startable) redis).stop();
        }
    }

    @BeforeEach
    void flushAll() throws TimeoutException, InterruptedException {
        redisCommands.flushall();
        awaitUntil(() -> redisCommands.pubsubNumpat() == 0);
    }

    public static TestInfo testInfo(TestInfo info, String... suffixes) {
        return new SimpleTestInfo(info, suffixes);
    }

    public <T> List<? extends T> readAll(TestInfo info, ItemReader<T> reader) throws JobExecutionException {
        ListItemWriter<T> writer = new ListItemWriter<>();
        run(testInfo(info, UUID.randomUUID().toString()), reader, writer);
        return writer.getWrittenItems();
    }

    public static void assertDbNotEmpty(RedisModulesCommands<String, String> commands) {
        Assertions.assertTrue(commands.dbsize() > 0, "Redis database is empty");
    }

    protected GeneratorItemReader generator(int count, ItemType... types) {
        GeneratorItemReader gen = new GeneratorItemReader();
        gen.setMaxItemCount(count);
        if (!ObjectUtils.isEmpty(types)) {
            gen.setTypes(types);
        }
        return gen;
    }

    protected RedisItemReader<byte[], byte[]> dumpReader(TestInfo info, String... suffixes) {
        return reader(info, redisClient, RedisItemReader.dump(), suffixes);
    }

    protected RedisItemReader<String, String> structReader(TestInfo info, String... suffixes) {
        return structReader(info, StringCodec.UTF8, suffixes);
    }

    protected <K, V> RedisItemReader<K, V> structReader(TestInfo info, RedisCodec<K, V> codec, String... suffixes) {
        return structReader(info, redisClient, codec, suffixes);
    }

    protected <K, V> RedisItemReader<K, V> structReader(TestInfo info, AbstractRedisClient client, RedisCodec<K, V> codec,
            String... suffixes) {
        return reader(info, client, RedisItemReader.struct(codec), suffixes);
    }

    private <K, V> RedisItemReader<K, V> reader(TestInfo info, AbstractRedisClient client, RedisItemReader<K, V> reader,
            String... suffixes) {
        List<String> allSuffixes = new ArrayList<>(Arrays.asList(suffixes));
        allSuffixes.add("reader");
        reader.setName(name(testInfo(info, allSuffixes.toArray(new String[0]))));
        reader.setJobRepository(jobRepository);
        reader.setClient(client);
        reader.setIdleTimeout(idleTimeout);
        return reader;
    }

    protected int keyCount(String pattern) {
        return redisCommands.keys(pattern).size();
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public Duration getAwaitPollDelay() {
        return awaitPollDelay;
    }

    public Duration getAwaitPollInterval() {
        return awaitPollInterval;
    }

    public Duration getAwaitTimeout() {
        return awaitTimeout;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setAwaitPollDelay(Duration pollDelay) {
        this.awaitPollDelay = pollDelay;
    }

    public void setAwaitPollInterval(Duration awaitPollInterval) {
        this.awaitPollInterval = awaitPollInterval;
    }

    public void setAwaitTimeout(Duration awaitTimeout) {
        this.awaitTimeout = awaitTimeout;
    }

    protected abstract RedisServer getRedisServer();

    protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, ItemReader<? extends I> reader, ItemWriter<O> writer) {
        return step(info, reader, null, writer);
    }

    protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, ItemReader<? extends I> reader, ItemProcessor<I, O> processor,
            ItemWriter<O> writer) {
        return step(info, chunkSize, reader, processor, writer);
    }

    protected <I, O> SimpleStepBuilder<I, O> step(TestInfo info, int chunkSize, ItemReader<? extends I> reader,
            ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        String name = name(info);
        SimpleStepBuilder<I, O> step = step(name, chunkSize);
        if (isLiveRedisItemReader(reader)) {
            enableKeyspaceNotifications();
            RedisItemReader<?, ?> redisReader = (RedisItemReader<?, ?>) reader;
            step = new FlushingStepBuilder<>(step).idleTimeout(redisReader.getIdleTimeout())
                    .flushInterval(redisReader.getFlushInterval());
        }
        step.reader(reader);
        step.processor(processor);
        step.writer(writer);
        return step;
    }

    private boolean isLiveRedisItemReader(ItemReader<?> reader) {
        return reader instanceof RedisItemReader && ((RedisItemReader<?, ?>) reader).getMode() != ReaderMode.SCAN;
    }

    protected <I, O> SimpleStepBuilder<I, O> step(String name, int chunkSize) {
        return new StepBuilder(name, jobRepository).chunk(chunkSize, transactionManager);
    }

    public static String name(TestInfo info) {
        StringBuilder displayName = new StringBuilder();
        info.getTestClass().ifPresent(c -> displayName.append(ClassUtils.getShortName(c)).append("-"));
        displayName.append(info.getDisplayName().replace("(TestInfo)", ""));
        return displayName.toString();
    }

    public static AbstractRedisClient client(RedisServer server) {
        if (server.isRedisCluster()) {
            return RedisModulesClusterClient.create(server.getRedisURI());
        }
        return RedisModulesClient.create(server.getRedisURI());
    }

    public void awaitRunning(JobExecution jobExecution) {
        awaitUntil(jobExecution::isRunning);
    }

    public void awaitTermination(JobExecution jobExecution) {
        awaitUntilFalse(jobExecution::isRunning);
    }

    protected void awaitUntilFalse(Callable<Boolean> evaluator) {
        awaitUntil(() -> !evaluator.call());
    }

    protected void awaitUntil(Callable<Boolean> evaluator) {
        Awaitility.await().pollDelay(awaitPollDelay).pollInterval(awaitPollInterval).timeout(awaitTimeout).until(evaluator);
    }

    protected JobBuilder job(TestInfo info) {
        return job(name(info));
    }

    protected JobBuilder job(String name) {
        return new JobBuilder(name, jobRepository);
    }

    protected void generateAsync(TestInfo info, GeneratorItemReader reader) throws InterruptedException, ExecutionException {
        executeWhenSubscribers(() -> generate(info, reader));
    }

    protected <T> void executeWhenSubscribers(Callable<T> callable) throws InterruptedException, ExecutionException {
        Executors.newSingleThreadExecutor().submit(() -> {
            awaitUntil(() -> redisCommands.pubsubNumpat() > 0);
            return callable.call();
        });
    }

    protected JobExecution generate(TestInfo info, GeneratorItemReader reader) throws JobExecutionException {
        return generate(info, redisClient, reader);
    }

    protected JobExecution generate(TestInfo info, AbstractRedisClient client, GeneratorItemReader reader)
            throws JobExecutionException {
        TestInfo testInfo = testInfo(info, "generate");
        RedisItemWriter<String, String, KeyValue<String>> writer = RedisItemWriter.struct(StringCodec.UTF8);
        writer.setClient(client);
        return run(testInfo, reader, writer);
    }

    protected <T> JobExecution run(TestInfo info, ItemReader<? extends T> reader, ItemWriter<T> writer)
            throws JobExecutionException {
        return run(info, reader, null, writer);
    }

    protected <I, O> JobExecution run(TestInfo info, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
            throws JobExecutionException {
        return run(info, step(info, reader, processor, writer));
    }

    protected <I, O> JobExecution run(TestInfo info, SimpleStepBuilder<I, O> step) throws JobExecutionException {
        return run(job(info).start(step.build()).build());
    }

    protected JobExecution run(Job job) throws JobExecutionException {
        JobExecution jobExecution;
        try {
            jobExecution = jobLauncher.run(job, new JobParameters());
        } catch (Exception e) {
            throw new JobExecutionException("Could not run job " + job.getName(), e);
        }
        if (!jobExecution.getAllFailureExceptions().isEmpty()) {
            throw new JobExecutionException("Could not run job " + job.getName(),
                    jobExecution.getAllFailureExceptions().get(0));
        }
        awaitUntilFalse(jobExecution::isRunning);
        return jobExecution;
    }

    protected void enableKeyspaceNotifications() {
        redisCommands.configSet("notify-keyspace-events", "AKE");
    }

    protected void generateStreams(TestInfo info, int streamCount, int messageCount)
            throws JobExecutionException, TimeoutException, InterruptedException {
        GeneratorItemReader gen = generator(streamCount, ItemType.STREAM);
        StreamOptions streamOptions = new StreamOptions();
        streamOptions.setMessageCount(new Range(messageCount, messageCount));
        gen.setStreamOptions(streamOptions);
        generate(info, gen);
    }

    protected StreamItemReader<String, String> streamReader(TestInfo info, Consumer<String> consumer, String... streams) {
        StreamItemReader<String, String> reader = new StreamItemReader<>(redisClient, StringCodec.UTF8, streams);
        reader.setConsumer(consumer);
        reader.setPollTimeout(Duration.ofMillis(300));
        reader.setName(name(testInfo(info, "stream-reader")));
        return reader;
    }

    protected void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
        for (StreamMessage<String, String> message : items) {
            assertTrue(message.getBody().containsKey("field1"));
            assertTrue(message.getBody().containsKey("field2"));
        }
    }

    protected void assertStreamEquals(String expectedId, Map<String, String> expectedBody, String expectedStream,
            StreamMessage<String, String> message) {
        Assertions.assertEquals(expectedId, message.getId());
        Assertions.assertEquals(expectedBody, message.getBody());
        Assertions.assertEquals(expectedStream, message.getStream());
    }

    protected Map<String, String> map(String... args) {
        Assert.notNull(args, "Args cannot be null");
        Assert.isTrue(args.length % 2 == 0, "Args length is not a multiple of 2");
        Map<String, String> body = new LinkedHashMap<>();
        for (int index = 0; index < args.length / 2; index++) {
            body.put(args[index * 2], args[index * 2 + 1]);
        }
        return body;
    }

    protected byte[] toByteArray(String key) {
        return BatchUtils.toByteArrayKeyFunction(StringCodec.UTF8).apply(key);
    }

    protected String toString(byte[] key) {
        return BatchUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE).apply(key);
    }

    protected <T> RedisItemWriter<String, String, T> writer(RedisOperation<String, String, T, Object> operation) {
        RedisItemWriter<String, String, T> writer = RedisItemWriter.operation(operation);
        writer.setClient(redisClient);
        return writer;
    }

}
