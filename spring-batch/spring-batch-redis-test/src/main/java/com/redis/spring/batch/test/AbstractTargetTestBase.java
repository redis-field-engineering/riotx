package com.redis.spring.batch.test;

import com.redis.batch.*;
import com.redis.batch.gen.StreamOptions;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.spring.batch.item.redis.GeneratorItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.reader.*;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.junit.jupiter.api.*;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.item.support.ListItemWriter;
import org.testcontainers.lifecycle.Startable;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractTargetTestBase extends AbstractTestBase {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMinutes(3);

    protected RedisURI targetRedisURI;

    protected AbstractRedisClient targetRedisClient;

    private StatefulRedisModulesConnection<String, String> targetRedisConnection;

    protected RedisModulesCommands<String, String> targetRedisCommands;

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    protected abstract RedisServer getTargetRedisServer();

    @BeforeAll
    void targetSetup() {
        // Target Redis setup
        RedisServer targetRedis = getTargetRedisServer();
        if (targetRedis instanceof Startable) {
            ((Startable) targetRedis).start();
        }
        targetRedisURI = redisURI(targetRedis);
        targetRedisClient = client(targetRedis);
        targetRedisConnection = targetRedisConnection(StringCodec.UTF8);
        targetRedisCommands = targetRedisConnection.sync();
        log.debug("Successfully set up target Redis:\n{}", targetRedisCommands.info());
    }

    @AfterAll
    void targetTeardown() {
        if (targetRedisConnection != null) {
            targetRedisConnection.close();
            targetRedisConnection = null;
        }
        if (targetRedisClient != null) {
            targetRedisClient.shutdown();
            targetRedisClient.getResources().shutdown();
            targetRedisClient = null;
        }
        RedisServer targetRedis = getTargetRedisServer();
        if (targetRedis instanceof Startable) {
            ((Startable) targetRedis).stop();
        }
    }

    @BeforeEach
    void targetFlushAll() {
        targetRedisCommands.flushall();
    }

    protected <K, V> StatefulRedisModulesConnection<K, V> targetRedisConnection(RedisCodec<K, V> codec) {
        return ConnectionBuilder.client(targetRedisClient).connection(codec);
    }

    protected void assertTtlEquals(Instant expected, Instant actual) {
        Assertions.assertEquals(expected.toEpochMilli(), actual.toEpochMilli(), ttlTolerance.toMillis());
    }

    public void setTtlTolerance(Duration tolerance) {
        this.ttlTolerance = tolerance;
    }

    protected <K, V, R extends RedisItemReader<K, V, ?>> R targetClient(R reader) {
        reader.setClient(targetRedisClient);
        return reader;
    }

    protected <K, V, T> RedisItemWriter<K, V, T> targetClient(RedisItemWriter<K, V, T> writer) {
        writer.setClient(targetRedisClient);
        return writer;
    }

    protected RedisItemWriter<String, String, KeyStructEvent<String, String>> structWriter() {
        return structWriter(StringCodec.UTF8);
    }

    protected <K, V> RedisItemWriter<K, V, KeyStructEvent<K, V>> structWriter(RedisCodec<K, V> codec) {
        return structWriter(targetRedisClient, codec);
    }

    protected <K, V> RedisItemWriter<K, V, KeyStructEvent<K, V>> structWriter(AbstractRedisClient client,
            RedisCodec<K, V> codec) {
        RedisItemWriter<K, V, KeyStructEvent<K, V>> writer = RedisItemWriter.struct(codec);
        writer.setClient(client);
        return writer;
    }

    protected RedisItemWriter<byte[], byte[], KeyDumpEvent<byte[]>> dumpWriter() {
        return targetClient(RedisItemWriter.dump());
    }

    protected void assertCompare(TestInfo info) throws Exception {
        List<KeyComparison<String>> comparisons = compare(info);
        Map<Status, List<KeyComparison<String>>> groups = comparisons.stream()
                .collect(Collectors.groupingBy(KeyComparison::getStatus));
        groups.forEach((s, g) -> {
            log.info(s + ": " + g.size());
        });
        Assertions.assertTrue(comparisons.stream().allMatch(s -> s.getStatus() == Status.OK));
    }

    protected List<KeyComparison<String>> compare(TestInfo info) throws JobExecutionException {
        return compare(info, StringCodec.UTF8);
    }

    protected <K, V> List<KeyComparison<K>> compare(TestInfo info, RedisCodec<K, V> codec) throws JobExecutionException {
        return compare(info, codec, comparator(codec));
    }

    protected <K, V> KeyStructComparator<K, V> comparator(RedisCodec<K, V> codec) {
        KeyStructComparator<K, V> comparator = new KeyStructComparator<>(codec);
        comparator.setTtlTolerance(Duration.ofMillis(100));
        return comparator;
    }

    protected <K, V> List<KeyComparison<K>> compare(TestInfo info, RedisCodec<K, V> codec,
            KeyComparator<K, KeyStructEvent<K, V>> comparator) throws JobExecutionException {
        assertDbNotEmpty(redisCommands);
        RedisScanItemReader<K, V, KeyStructEvent<K, V>> sourceReader = client(RedisScanItemReader.struct(codec));
        RedisScanItemReader<K, V, KeyStructEvent<K, V>> targetReader = targetClient(RedisScanItemReader.struct(codec));
        KeyComparisonItemReader<K, V> reader = new KeyComparisonItemReader<>(sourceReader, targetReader);
        reader.setComparator(comparator);
        ListItemWriter<KeyComparison<K>> writer = new ListItemWriter<>();
        run(testInfo(info, "compare"), reader, writer);
        return writer.getWrittenItems();
    }

    protected <K, V, T extends KeyTtlTypeEvent<K>> List<KeyComparison<String>> replicate(TestInfo info,
            RedisItemReader<K, V, T> reader, RedisItemWriter<K, V, T> writer) throws Exception {
        if (reader instanceof RedisScanItemReader) {
            generate(info, generator(130));
        } else {
            GeneratorItemReader generator = generator(1, KeyType.stream);
            StreamOptions streamOptions = generator.getGenerator().getStreamOptions();
            streamOptions.setMessageCount(Range.of(3));
            streamOptions.getBodyOptions().setFieldCount(Range.of(1));
            generateAsync(info, generator);
        }
        return runAndCompare(info, reader, writer);
    }

    protected <K, V, T extends KeyTtlTypeEvent<K>> List<KeyComparison<String>> runAndCompare(TestInfo info,
            RedisItemReader<K, V, T> reader, RedisItemWriter<K, V, T> writer) throws Exception {
        TestInfo replicateInfo = testInfo(info, "replicate");
        run(replicateInfo, reader, writer);
        List<KeyComparison<String>> comparisons = compare(replicateInfo);
        List<KeyComparison<String>> mismatches = comparisons.stream().filter(s -> s.getStatus() != Status.OK)
                .collect(Collectors.toList());
        Assertions.assertEquals(Collections.emptyList(), mismatches);
        return comparisons;
    }

}
