package com.redis.batch;

import com.redis.batch.gen.Generator;
import com.redis.batch.operation.AbstractKeyValueRead;
import com.redis.batch.operation.KeyDumpRead;
import com.redis.batch.operation.KeyStructWrite;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(Lifecycle.PER_CLASS)
public class KeyOperationTests {

    private static final RedisContainer redis = new RedisContainer(
            RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected AbstractRedisClient redisClient;

    private StatefulRedisModulesConnection<String, String> redisConnection;

    private RedisModulesCommands<String, String> redisCommands;

    @BeforeAll
    void setup() throws Exception {
        redis.start();
        redisClient = client(redis);
        redisConnection = ConnectionBuilder.client(redisClient).connection();
        redisCommands = redisConnection.sync();
    }

    public static AbstractRedisClient client(RedisServer server) {
        if (server.isRedisCluster()) {
            return RedisModulesClusterClient.create(server.getRedisURI());
        }
        return RedisModulesClient.create(server.getRedisURI());
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
        redis.stop();
    }

    @BeforeEach
    void flushAll() {
        redisCommands.flushall();
    }

    @Test
    void testKeyValueReadDump() throws Exception {
        redisCommands.set("key", "value");
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        try (StatefulRedisModulesConnection<byte[], byte[]> bytesConnection = ConnectionBuilder.client(redisClient)
                .connection(ByteArrayCodec.INSTANCE);
                OperationExecutor<byte[], byte[], KeyEvent<byte[]>, KeyDumpEvent<byte[]>> executor = new OperationExecutor<>(
                        ByteArrayCodec.INSTANCE, new KeyDumpRead())) {
            executor.setClient(redisClient);
            executor.afterPropertiesSet();
            KeyEvent<byte[]> keyEvent = new KeyEvent<>();
            keyEvent.setTimestamp(Instant.now());
            keyEvent.setKey(key);
            List<KeyDumpEvent<byte[]>> values = executor.execute(Collections.singletonList(keyEvent));
            Assertions.assertEquals(1, values.size());
            Assertions.assertArrayEquals(bytesConnection.sync().dump(key), values.get(0).getDump());
        }
    }

    @Test
    void testKeyValueReadDumps() throws Exception {
        Generator gen = new Generator();
        List<KeyStructEvent<String, String>> items = new ArrayList<>();
        for (int i = 0; i < 123; i++) {
            items.add(gen.next());
        }
        try (OperationExecutor<String, String, KeyStructEvent<String, String>, Object> executor = new OperationExecutor<>(
                StringCodec.UTF8, new KeyStructWrite<>())) {
            executor.setClient(redisClient);
            executor.afterPropertiesSet();
            executor.execute(items);
        }
        Assertions.assertEquals(items.size(), redisCommands.dbsize());
        try (StatefulRedisModulesConnection<byte[], byte[]> bytesConnection = ConnectionBuilder.client(redisClient)
                .connection(ByteArrayCodec.INSTANCE);
                OperationExecutor<byte[], byte[], KeyEvent<byte[]>, KeyDumpEvent<byte[]>> executor = new OperationExecutor<>(
                        ByteArrayCodec.INSTANCE, new KeyDumpRead())) {
            executor.setClient(redisClient);
            executor.afterPropertiesSet();
            List<KeyEvent<byte[]>> keys = items.stream().map(e -> {
                KeyEvent<byte[]> keyEvent = new KeyEvent<>();
                keyEvent.setTimestamp(Instant.now());
                keyEvent.setKey(e.getKey().getBytes());
                return keyEvent;
            }).collect(Collectors.toList());
            List<KeyDumpEvent<byte[]>> dumps = executor.execute(keys);
            Assertions.assertEquals(items.size(), dumps.size());
            for (KeyDumpEvent<byte[]> dump : dumps) {
                Assertions.assertArrayEquals(bytesConnection.sync().dump(dump.getKey()), dump.getDump());
            }
        }
    }

}
