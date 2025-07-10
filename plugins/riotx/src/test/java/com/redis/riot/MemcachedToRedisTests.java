package com.redis.riot;

import com.redis.batch.Range;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.riot.core.InetSocketAddressList;
import com.redis.spring.batch.JobUtils;
import com.redis.spring.batch.memcached.MemcachedEntry;
import com.redis.spring.batch.memcached.MemcachedGeneratorItemReader;
import com.redis.spring.batch.memcached.MemcachedItemReader;
import com.redis.spring.batch.memcached.MemcachedItemWriter;
import com.redis.testcontainers.MemcachedContainer;
import com.redis.testcontainers.MemcachedServer;
import com.redis.testcontainers.RedisContainer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import net.spy.memcached.MemcachedClient;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.simple.SimpleLogger;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.util.ClassUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

@TestInstance(Lifecycle.PER_CLASS)
public class MemcachedToRedisTests {

    static {
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SLF4JLogger");
        System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
    }

    private static final MemcachedContainer source = new MemcachedContainer(
            MemcachedContainer.DEFAULT_IMAGE_NAME.withTag(MemcachedContainer.DEFAULT_TAG));

    private static final RedisContainer target = new RedisContainer(
            RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));

    private JobRepository jobRepository;

    private Supplier<MemcachedClient> clientSupplier;

    private MemcachedClient client;

    private RedisModulesClient targetClient;

    private StatefulRedisModulesConnection<String, String> targetConnection;

    private StatefulRedisModulesConnection<byte[], byte[]> targetByteConnection;

    public static String name(TestInfo info) {
        StringBuilder displayName = new StringBuilder(info.getDisplayName().replace("(TestInfo)", ""));
        info.getTestClass().ifPresent(c -> displayName.append("-").append(ClassUtils.getShortName(c)));
        return displayName.toString();
    }

    public static <T> List<T> readAll(ItemReader<T> reader) throws Exception {
        List<T> list = new ArrayList<>();
        T element;
        while ((element = reader.read()) != null) {
            list.add(element);
        }
        return list;
    }

    @BeforeAll
    void setup() throws Exception {
        jobRepository = JobUtils.jobRepositoryFactoryBean(ClassUtils.getShortName(getClass())).getObject();
        source.start();
        target.start();
        clientSupplier = () -> client(source.getMemcachedAddresses());
        client = clientSupplier.get();
        targetClient = RedisModulesClient.create(target.getRedisURI());
        targetConnection = ConnectionBuilder.client(targetClient).connection();
        targetByteConnection = ConnectionBuilder.client(targetClient).connection(ByteArrayCodec.INSTANCE);
    }

    private static MemcachedClient client(List<InetSocketAddress> addresses) {
        try {
            return new MemcachedClient(addresses);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    void teardown() {
        if (client != null) {
            client.shutdown();
        }
        source.stop();
        if (targetByteConnection != null) {
            targetByteConnection.close();
        }
        if (targetConnection != null) {
            targetConnection.close();
        }
        if (targetClient != null) {
            targetClient.shutdown();
        }
        target.stop();
    }

    @BeforeEach
    void flushall() {
        client.flush();
        targetConnection.sync().flushall();
    }

    private List<MemcachedEntry> readAll(Supplier<MemcachedClient> clientSupplier) throws Exception {
        MemcachedItemReader reader = new MemcachedItemReader(clientSupplier);
        reader.setName(UUID.randomUUID().toString());
        try {
            reader.open(new ExecutionContext());
            return readAll(reader);
        } finally {
            reader.close();
        }
    }

    private void write(List<MemcachedEntry> entries) throws Exception {
        MemcachedItemWriter writer = new MemcachedItemWriter(clientSupplier);
        try {
            writer.open(new ExecutionContext());
            writer.write(new Chunk<>(entries));
        } finally {
            writer.close();
        }
    }

    private List<MemcachedEntry> entries(int count) throws Exception {
        MemcachedGeneratorItemReader reader = new MemcachedGeneratorItemReader();
        reader.setExpiration(new Range(100, 130));
        reader.setMaxItemCount(count);
        try {
            reader.open(new ExecutionContext());
            return readAll(reader);
        } finally {
            reader.close();
        }

    }

    @Test
    void replicate() throws Exception {
        int count = 123;
        write(entries(count));
        MemcachedReplicate replication = new MemcachedReplicate();
        replication.getProgressArgs().setStyle(ProgressStyle.NONE);
        replication.getJobExecutor().setJobRepository(jobRepository);
        replication.setSourceAddressList(new InetSocketAddressList(inetSocketAddress(source)));
        replication.setTargetUri(RedisURI.create(target.getRedisURI()));
        replication.setTargetType(MemcachedReplicate.ServerType.REDIS);
        replication.call();
        Instant startTime = Instant.now();
        List<MemcachedEntry> sourceEntries = readAll(clientSupplier);
        for (MemcachedEntry entry : sourceEntries) {
            byte[] targetValue = targetByteConnection.sync()
                    .get(ByteArrayCodec.INSTANCE.decodeKey(StringCodec.UTF8.encodeKey(entry.getKey())));
            Assertions.assertArrayEquals(entry.getValue(), targetValue);
            Assertions.assertEquals(entry.getExpiration().getEpochSecond(),
                    startTime.plusSeconds(targetConnection.sync().ttl(entry.getKey())).getEpochSecond(), 10);
        }
    }

    private InetSocketAddress inetSocketAddress(MemcachedServer server) {
        return InetSocketAddress.createUnresolved(server.getMemcachedHost(), server.getMemcachedPort());
    }

}
