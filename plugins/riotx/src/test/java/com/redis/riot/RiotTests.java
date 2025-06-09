package com.redis.riot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.batch.gen.ItemType;
import com.redis.lettucemod.Beers;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.riot.core.CompareMode;
import com.redis.riot.core.ReplicationMode;
import com.redis.riot.replicate.Replicate;
import com.redis.riot.replicate.ReplicateWriteLogger;
import com.redis.batch.KeyValue;
import com.redis.batch.Range;
import com.redis.spring.batch.item.redis.GeneratorItemReader;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.ByteArrayCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;
import org.testcontainers.shaded.org.bouncycastle.util.encoders.Hex;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;

abstract class RiotTests extends AbstractRiotApplicationTestBase {

    @BeforeAll
    void setDefaults() {
        setIdleTimeout(Duration.ofSeconds(1));
    }

    protected void runLiveReplication(TestInfo info, String filename) throws Exception {
        ItemType[] types = new ItemType[] { ItemType.HASH, ItemType.STRING };
        enableKeyspaceNotifications();
        generate(info, generator(3000, types));
        GeneratorItemReader generator = generator(3500, types);
        generator.setCurrentItemCount(3001);
        generateAsync(testInfo(info, "async"), generator);
        execute(info, filename);
        assertCompare(info);
    }

    protected static String name(Map<String, String> beer) {
        return beer.get("name");
    }

    protected static String style(Map<String, String> beer) {
        return beer.get("style");
    }

    protected static double abv(Map<String, String> beer) {
        return Double.parseDouble(beer.get("abv"));
    }

    protected void execute(Replicate replicate, TestInfo info) throws Exception {
        System.setProperty(SimpleLogger.LOG_KEY_PREFIX + ReplicateWriteLogger.class.getName(), "error");
        replicate.getProgressArgs().setStyle(ProgressStyle.NONE);
        replicate.getJobExecutor().setJobRepository(jobRepository);
        replicate.setSourceRedisUri(redisURI);
        replicate.getSourceRedisArgs().setCluster(getRedisServer().isRedisCluster());
        replicate.setTargetRedisUri(targetRedisURI);
        replicate.getTargetRedisArgs().setCluster(getTargetRedisServer().isRedisCluster());
        replicate.getFlushingStepArgs().setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
        replicate.call();
    }

    @Test
    void replicateBinaryStruct(TestInfo info) throws Exception {
        byte[] key = Hex.decode("aced0005");
        byte[] value = Hex.decode("aced0004");
        Map<byte[], byte[]> hash = new HashMap<>();
        hash.put(key, value);
        try (StatefulRedisModulesConnection<byte[], byte[]> connection = ConnectionBuilder.client(redisClient)
                .connection(ByteArrayCodec.INSTANCE);
                StatefulRedisModulesConnection<byte[], byte[]> targetConnection = ConnectionBuilder.client(targetRedisClient)
                        .connection(ByteArrayCodec.INSTANCE)) {
            connection.sync().hset(key, hash);
            Replicate replication = new Replicate();
            replication.setCompareMode(CompareMode.NONE);
            replication.setStruct(true);
            execute(replication, info);
            Assertions.assertArrayEquals(connection.sync().hget(key, key), targetConnection.sync().hget(key, key));
        }
    }

    @Test
    void replicateBinaryKeyValueScan(TestInfo info) throws Exception {
        byte[] key = Hex.decode("aced0005");
        byte[] value = Hex.decode("aced0004");
        try (StatefulRedisModulesConnection<byte[], byte[]> connection = ConnectionBuilder.client(redisClient)
                .connection(ByteArrayCodec.INSTANCE);
                StatefulRedisModulesConnection<byte[], byte[]> targetConnection = ConnectionBuilder.client(targetRedisClient)
                        .connection(ByteArrayCodec.INSTANCE)) {
            connection.sync().set(key, value);
            Replicate replication = new Replicate();
            replication.setCompareMode(CompareMode.NONE);
            execute(replication, info);
            Assertions.assertArrayEquals(connection.sync().get(key), targetConnection.sync().get(key));
        }
    }

    @Test
    void replicateBinaryKeyLive(TestInfo info) throws Exception {
        byte[] key = Hex.decode("aced0005");
        byte[] value = Hex.decode("aced0004");
        try (StatefulRedisModulesConnection<byte[], byte[]> connection = ConnectionBuilder.client(redisClient)
                .connection(ByteArrayCodec.INSTANCE);
                StatefulRedisModulesConnection<byte[], byte[]> targetConnection = ConnectionBuilder.client(targetRedisClient)
                        .connection(ByteArrayCodec.INSTANCE)) {
            enableKeyspaceNotifications();
            executeWhenSubscribers(() -> connection.sync().set(key, value));
            Replicate replicate = new Replicate();
            replicate.setMode(ReplicationMode.LIVE);
            replicate.setCompareMode(CompareMode.NONE);
            execute(replicate, info);
            Assertions.assertArrayEquals(connection.sync().get(key), targetConnection.sync().get(key));
        }
    }

    @Test
    void filterKeySlot(TestInfo info) throws Exception {
        enableKeyspaceNotifications();
        Replicate replication = new Replicate();
        replication.setMode(ReplicationMode.LIVE);
        replication.setCompareMode(CompareMode.NONE);
        replication.getReaderArgs().getKeyFilterArgs().setSlots(Arrays.asList(new Range(0, 8000)));
        generateAsync(info, generator(100));
        execute(replication, info);
        Assertions.assertTrue(targetRedisCommands.keys("*").stream().map(SlotHash::getSlot).allMatch(between(0, 8000)));
    }

    private Predicate<Integer> between(int start, int end) {
        return i -> i >= start && i <= end;
    }

    @Test
    void redisImportJson(TestInfo info) throws Exception {
        int count = 1000;
        GeneratorItemReader generator = generator(count, ItemType.HASH);
        generator.getGenerator().setKeyspace("hash");
        generate(info, generator);
        String filename = "redis-import-json";
        execute(info, filename);
        List<String> keys = targetRedisCommands.keys("doc:*");
        Assertions.assertEquals(count, keys.size());
        Assertions.assertEquals(KeyValue.TYPE_JSON, targetRedisCommands.type(keys.get(0)));
        String json = targetRedisCommands.jsonGet(keys.get(1)).get(0).toString();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(json);
        Assertions.assertNotNull(node.get("field1"));
    }

    @Test
    void streamImport(TestInfo info) throws Exception {
        String stream = "stream:beers";
        populateStream(stream);
        String filename = "stream-import-hset";
        execute(info, filename);
        assertStreamImport(redisCommands);
    }

    @Test
    void streamImportTarget(TestInfo info) throws Exception {
        String stream = "stream:beers";
        populateStream(stream);
        String filename = "stream-import-target-hset";
        execute(info, filename);
        assertStreamImport(targetRedisCommands);
    }

    @Test
    void parquetFileImport(TestInfo info) throws Exception {
        String filename = "file-import-parquet";
        execute(info, filename);
        Assertions.assertEquals(1000, redisCommands.dbsize());
    }

    private static void assertStreamImport(RedisCommands<String, String> commands) throws IOException {
        List<String> keys = commands.keys("beer:*");
        Assertions.assertEquals(1019, keys.size());
        Map<String, Object> expected = Beers.mapIterator().next();
        Map<String, String> actual = commands.hgetall("beer:" + expected.get("id"));
        Assertions.assertEquals(expected, actual);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private int populateStream(String stream) throws IOException {
        List<RedisFuture<?>> futures = new ArrayList<>();
        int count = 0;
        try {
            MappingIterator<Map<String, Object>> iterator = Beers.mapIterator();
            while (iterator.hasNext()) {
                Map<String, String> beer = (Map) iterator.next();
                futures.add(redisAsyncCommands.xadd(stream, beer));
                count++;
            }
            redisConnection.flushCommands();
            LettuceFutures.awaitAll(redisConnection.getTimeout(), futures.toArray(new RedisFuture[0]));
        } finally {
            redisConnection.setAutoFlushCommands(true);
        }
        return count;
    }

}
