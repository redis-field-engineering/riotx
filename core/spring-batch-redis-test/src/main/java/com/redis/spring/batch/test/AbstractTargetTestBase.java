package com.redis.spring.batch.test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecutionException;
import org.testcontainers.lifecycle.Startable;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.Range;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.ItemType;
import com.redis.spring.batch.item.redis.reader.DefaultKeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemWriter;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

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
        log.info("Successfully set up target Redis:\n{}", targetRedisCommands.info());
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
        return BatchUtils.connection(targetRedisClient, codec);
    }

    protected void assertTtlEquals(long expected, long actual) {
        Assertions.assertEquals(expected, actual, ttlTolerance.toMillis());
    }

    public void setTtlTolerance(Duration tolerance) {
        this.ttlTolerance = tolerance;
    }

    protected RedisItemWriter<String, String, KeyValue<String>> structWriter() {
        return structWriter(StringCodec.UTF8);
    }

    protected <K, V> RedisItemWriter<K, V, KeyValue<K>> structWriter(RedisCodec<K, V> codec) {
        return structWriter(targetRedisClient, codec);
    }

    protected <K, V> RedisItemWriter<K, V, KeyValue<K>> structWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        RedisItemWriter<K, V, KeyValue<K>> writer = RedisItemWriter.struct(codec);
        writer.setClient(client);
        return writer;
    }

    protected RedisItemWriter<byte[], byte[], KeyValue<byte[]>> dumpWriter() {
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = RedisItemWriter.dump();
        writer.setClient(targetRedisClient);
        return writer;
    }

    protected void assertCompare(TestInfo info) throws Exception {
        List<KeyComparison<String>> comparisons = compare(info);
        Assertions.assertTrue(comparisons.stream().allMatch(s -> s.getStatus() == Status.OK));
    }

    protected void logDiffs(Collection<KeyComparison<String>> diffs) {
        for (KeyComparison<String> diff : diffs) {
            log.error("{}: {} {}", diff.getStatus(), diff.getSource().getKey(), diff.getSource().getType());
        }
    }

    protected List<KeyComparison<String>> compare(TestInfo info) throws JobExecutionException {
        return compare(info, StringCodec.UTF8);
    }

    protected <K, V> List<KeyComparison<K>> compare(TestInfo info, RedisCodec<K, V> codec) throws JobExecutionException {
        return compare(info, codec, comparator(codec));
    }

    protected <K, V> DefaultKeyComparator<K, V> comparator(RedisCodec<K, V> codec) {
        DefaultKeyComparator<K, V> comparator = new DefaultKeyComparator<>(codec);
        comparator.setTtlTolerance(Duration.ofMillis(100));
        return comparator;
    }

    protected <K, V> List<KeyComparison<K>> compare(TestInfo info, RedisCodec<K, V> codec, KeyComparator<K> comparator)
            throws JobExecutionException {
        assertDbNotEmpty(redisCommands);
        RedisItemReader<K, V> sourceReader = structReader(testInfo(info, "comparisonReader", "source"), codec);
        RedisItemReader<K, V> targetReader = structReader(testInfo(info, "comparisonReader", "target"), targetRedisClient,
                codec);
        targetReader.setClient(targetRedisClient);
        KeyComparisonItemWriter<K, V> writer = new KeyComparisonItemWriter<>(targetReader, comparator);
        List<KeyComparison<K>> comparisons = new ArrayList<>();
        writer.addListener(comparisons::add);
        writer.setName(name(testInfo(info, "comparisonReader")));
        run(testInfo(info, "compare"), sourceReader, writer);
        return comparisons;
    }

    protected <K, V> List<KeyComparison<String>> replicate(TestInfo info, RedisItemReader<K, V> reader,
            RedisItemWriter<K, V, KeyValue<K>> writer) throws Exception {
        if (reader.getMode() != ReaderMode.LIVEONLY) {
            generate(info, generator(130));
        }
        if (reader.getMode() != ReaderMode.SCAN) {
            GeneratorItemReader liveGen = generator(70, ItemType.HASH, ItemType.STRING);
            liveGen.setKeyRange(new Range(300, 500));
            generateAsync(testInfo(info, "genasync"), liveGen);
        }
        return runAndCompare(info, reader, writer);
    }

    protected <K, V> List<KeyComparison<String>> runAndCompare(TestInfo info, RedisItemReader<K, V> reader,
            RedisItemWriter<K, V, KeyValue<K>> writer) throws Exception {
        run(testInfo(info, "replicate"), reader, writer);
        List<KeyComparison<String>> comparisons = compare(testInfo(info, "replicate"));
        List<KeyComparison<String>> mismatches = comparisons.stream().filter(s -> s.getStatus() != Status.OK)
                .collect(Collectors.toList());
        if (!mismatches.isEmpty()) {
            List<String> sourceKeys = BatchUtils.connection(reader.getClient()).sync().keys("*");
            Collections.sort(sourceKeys);
            log.info("Source keys: {}", sourceKeys);
            List<String> targetKeys = BatchUtils.connection(writer.getClient()).sync().keys("*");
            Collections.sort(targetKeys);
            log.info("Target keys: {}", targetKeys);
        }
        Assertions.assertEquals(Collections.emptyList(), mismatches);
        return comparisons;
    }

}
