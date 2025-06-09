package com.redis.batch;

import com.redis.batch.gen.Generator;
import com.redis.batch.operation.KeyValueRead;
import com.redis.batch.operation.KeyValueWrite;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(Lifecycle.PER_CLASS)
public class OperationTests {

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
                OperationExecutor<byte[], byte[], byte[], KeyValue<byte[]>> executor = new OperationExecutor<>(
                        ByteArrayCodec.INSTANCE, KeyValueRead.dump())) {
            executor.setClient(redisClient);
            executor.afterPropertiesSet();
            List<KeyValue<byte[]>> values = executor.execute(Collections.singletonList(key));
            Assertions.assertEquals(1, values.size());
            Assertions.assertArrayEquals(bytesConnection.sync().dump(key), (byte[]) values.get(0).getValue());
        }
    }

    @Test
    void testKeyValueReadDumps() throws Exception {
        Generator gen = new Generator();
        List<KeyValue<String>> items = new ArrayList<>();
        for (int i = 0; i < 123; i++) {
            items.add(gen.next());
        }
        try (OperationExecutor<String, String, KeyValue<String>, Object> executor = new OperationExecutor<>(StringCodec.UTF8,
                new KeyValueWrite<>())) {
            executor.setClient(redisClient);
            executor.afterPropertiesSet();
            executor.execute(items);
        }
        Assertions.assertEquals(items.size(), redisCommands.dbsize());
        try (StatefulRedisModulesConnection<byte[], byte[]> bytesConnection = ConnectionBuilder.client(redisClient)
                .connection(ByteArrayCodec.INSTANCE);
                OperationExecutor<byte[], byte[], byte[], KeyValue<byte[]>> executor = new OperationExecutor<>(
                        ByteArrayCodec.INSTANCE, KeyValueRead.dump())) {
            executor.setClient(redisClient);
            executor.afterPropertiesSet();
            List<byte[]> keys = items.stream().map(KeyValue::getKey).map(String::getBytes).collect(Collectors.toList());
            List<KeyValue<byte[]>> dumps = executor.execute(keys);
            Assertions.assertEquals(items.size(), dumps.size());
            for (KeyValue<byte[]> dump : dumps) {
                Assertions.assertArrayEquals(bytesConnection.sync().dump(dump.getKey()), (byte[]) dump.getValue());
            }
        }
    }

}
