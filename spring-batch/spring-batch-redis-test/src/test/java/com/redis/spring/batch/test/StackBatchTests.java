package com.redis.spring.batch.test;

import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.timeseries.*;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.MultiOperation;
import com.redis.spring.batch.item.redis.common.Range;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.ItemType;
import com.redis.spring.batch.item.redis.gen.MapOptions;
import com.redis.spring.batch.item.redis.gen.TimeSeriesOptions;
import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;
import com.redis.spring.batch.item.redis.reader.RedisLiveItemReader;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import com.redis.spring.batch.item.redis.reader.StreamItemReader;
import com.redis.spring.batch.item.redis.reader.StreamItemReader.AckPolicy;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite.WriteMode;
import com.redis.spring.batch.item.redis.writer.impl.*;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.*;
import io.lettuce.core.Range.Boundary;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.unit.DataSize;

import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StackBatchTests extends BatchTests {

    private static final RedisServer source = RedisContainerFactory.stack();

    private static final RedisServer target = RedisContainerFactory.stack();

    @Override
    protected RedisServer getRedisServer() {
        return source;
    }

    @Override
    protected RedisServer getTargetRedisServer() {
        return target;
    }

    protected RedisLiveItemReader<String, String> liveStructReader() {
        return client(RedisLiveItemReader.struct());
    }

    @Test
    void replicateEmptyStream(TestInfo info) throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("test", "test");
        redisCommands.xadd(key, body);
        redisCommands.xtrim(key, 0);
        runAndCompare(info, scanStructReader(), structWriter());
    }

    @Test
    void compareTimeseries(TestInfo info) throws Exception {
        int count = 123;
        for (int index = 0; index < count; index++) {
            redisCommands.tsAdd("ts:" + index, Sample.of(123));
        }
        List<KeyComparison<String>> stats = compare(info);
        Assertions.assertEquals(count, stats.stream().filter(s -> s.getStatus() == Status.MISSING).count());
    }

    @Test
    void readStreamsAck(TestInfo info) throws Exception {
        readStreamsAck(info, 3);
    }

    @Test
    void readStreams(TestInfo info) throws Exception {
        readStreams(info, 3);
    }

    @Test
    void readStreamsConsumer(TestInfo info) throws Exception {
        readStreamsConsumer(info, 3);
    }

    @Test
    void readStreamsAutoAck(TestInfo info) throws Exception {
        readStreamsAutoAck(info, 3);
    }

    @Test
    void replicateHLL(TestInfo info) throws Exception {
        String key1 = "hll:1";
        redisCommands.pfadd(key1, "member:1", "member:2");
        String key2 = "hll:2";
        redisCommands.pfadd(key2, "member:1", "member:2", "member:3");
        RedisScanItemReader<byte[], byte[]> reader = scanStructReader(ByteArrayCodec.INSTANCE);
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = structWriter(ByteArrayCodec.INSTANCE);
        replicate(info, reader, writer);
        assertEquals(redisCommands.pfcount(key1), targetRedisCommands.pfcount(key1));
    }

    @Test
    void readLiveThreads(TestInfo info) throws Exception {
        RedisLiveItemReader<String, String> reader = liveStructReader();
        ListItemWriter<KeyValue<String>> writer = new ListItemWriter<>();
        int count = 123;
        generateAsync(info, generator(count));
        run(info, reader, writer);
        Map<String, List<KeyValue<String>>> groups = writer.getWrittenItems().stream()
                .collect(Collectors.groupingBy(KeyValue::getKey));
        Assertions.assertEquals(count, groups.size());
    }

    @Test
    void readLiveType(TestInfo info) throws Exception {
        generateAsync(info, generator(100));
        RedisLiveItemReader<String, String> reader = liveStructReader();
        reader.setKeyType(ItemType.HASH.getString());
        ListItemWriter<KeyValue<String>> writer = new ListItemWriter<>();
        run(info, reader, writer);
        writer.getWrittenItems().forEach(v -> Assertions.assertEquals(KeyValue.TYPE_HASH, v.getType()));
    }

    @Test
    void readStructMemoryUsage(TestInfo info) throws Exception {
        generate(info, generator(73));
        RedisScanItemReader<String, String> reader = scanStructReader();
        long memLimit = 200;
        reader.setMemoryLimit(memLimit);
        List<? extends KeyValue<String>> keyValues = readAll(info, reader);
        Assertions.assertFalse(keyValues.isEmpty());
        for (KeyValue<String> keyValue : keyValues) {
            if (keyValue.getMemoryUsage() > memLimit) {
                Assertions.assertNull(keyValue.getValue());
            }
        }
    }

    @Test
    void readStructMemoryUsageTTL(TestInfo info) throws Exception {
        String key = "myhash";
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value1");
        hash.put("field2", "value2");
        redisCommands.hset(key, hash);
        long ttl = System.currentTimeMillis() + 123456;
        redisCommands.pexpireat(key, ttl);
        RedisScanItemReader<String, String> reader = scanStructReader();
        reader.setMemoryLimit(-1);
        reader.open(new ExecutionContext());
        KeyValue<String> ds = reader.read();
        Assertions.assertEquals(key, ds.getKey());
        assertTtlEquals(ttl, ds.getTtl());
        Assertions.assertEquals(KeyValue.TYPE_HASH, ds.getType());
        Assertions.assertTrue(ds.getMemoryUsage() > 0);
        reader.close();
    }

    @Test
    void readStructMemLimit(TestInfo info) throws Exception {
        long limit = 500;
        String key1 = "key:1";
        redisCommands.set(key1, "bar");
        String key2 = "key:2";
        redisCommands.set(key2, GeneratorItemReader.string(Math.toIntExact(limit * 2)));
        RedisScanItemReader<String, String> reader = scanStructReader();
        reader.setMemoryLimit(limit);
        List<? extends KeyValue<String>> keyValues = readAll(info, reader);
        Map<String, KeyValue<String>> map = keyValues.stream().collect(Collectors.toMap(KeyValue::getKey, Function.identity()));
        Assertions.assertNull(map.get(key2).getValue());
    }

    @Test
    void replicateStructByteArray(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(1000);
        generate(info, gen);
        replicate(info, scanStructReader(ByteArrayCodec.INSTANCE), structWriter(ByteArrayCodec.INSTANCE));
    }

    @Test
    void replicateStructMemLimit(TestInfo info) throws Exception {
        generate(info, generator(73));
        RedisScanItemReader<String, String> reader = scanStructReader();
        reader.setMemoryLimit(DataSize.ofMegabytes(100).toBytes());
        replicate(info, reader, structWriter());
    }

    @Test
    void replicateDumpMemLimitHigh(TestInfo info) throws Exception {
        generate(info, generator(73));
        RedisScanItemReader<byte[], byte[]> reader = scanDumpReader();
        reader.setMemoryLimit(DataSize.ofMegabytes(100).toBytes());
        replicate(info, reader, dumpWriter());
    }

    @Test
    void replicateStructEmptyCollections(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(123);
        Range cardinality = new Range(0, 0);
        gen.getHashOptions().setFieldCount(cardinality);
        gen.getSetOptions().setMemberCount(cardinality);
        gen.getStreamOptions().setMessageCount(cardinality);
        gen.getTimeSeriesOptions().setSampleCount(cardinality);
        gen.getZsetOptions().setMemberCount(cardinality);
        generate(info, gen);
        replicate(info, scanStructReader(), structWriter());
    }

    @Test
    void readDumpMemLimitLow(TestInfo info) throws Exception {
        generate(info, generator(73));
        Assertions.assertTrue(redisCommands.dbsize() > 10);
        long memLimit = 1500;
        RedisScanItemReader<byte[], byte[]> reader = scanDumpReader();
        reader.setMemoryLimit(memLimit);
        List<? extends KeyValue<byte[]>> items = readAll(info, reader);
        Assertions.assertFalse(items.stream().anyMatch(v -> v.getMemoryUsage() > memLimit && v.getValue() != null));
    }

    @Test
    void writeStruct(TestInfo info) throws Exception {
        int count = 73;
        run(info, generator(count), structWriter(redisClient, StringCodec.UTF8));
        awaitUntil(() -> keyCount("gen:*") == count);
        assertEquals(count, keyCount("gen:*"));
    }

    @Test
    void writeStructMultiExec(TestInfo info) throws Exception {
        int count = 10;
        GeneratorItemReader reader = generator(count);
        RedisItemWriter<String, String, KeyValue<String>> writer = structWriter(redisClient, StringCodec.UTF8);
        writer.setMultiExec(true);
        run(info, step(info, 1, reader, null, writer));
        assertEquals(count, redisCommands.dbsize());
    }

    @Test
    void writeStreamMultiExec(TestInfo testInfo) throws Exception {
        String stream = "stream:1";
        List<Map<String, String>> messages = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("field1", "value1");
            body.put("field2", "value2");
            messages.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
        Xadd<String, String, Map<String, String>> xadd = new Xadd<>(t -> stream,
                t -> Arrays.asList(new StreamMessage<>(null, null, t)));
        xadd.setArgs(null);
        RedisItemWriter<String, String, Map<String, String>> writer = writer(xadd);
        writer.setMultiExec(true);
        run(testInfo, reader, writer);
        Assertions.assertEquals(messages.size(), redisCommands.xlen(stream));
        List<StreamMessage<String, String>> xrange = redisCommands.xrange(stream, io.lettuce.core.Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    @Test
    void readMultipleStreams(TestInfo info) throws Exception {
        String consumerGroup = "consumerGroup";
        generateStreams(info, 3, 277);
        KeyScanArgs args = KeyScanArgs.Builder.type(ItemType.STREAM.getString());
        final List<String> keys = ScanIterator.scan(redisCommands, args).stream().collect(Collectors.toList());
        for (String key : keys) {
            long count = redisCommands.xlen(key);
            StreamItemReader<String, String> reader1 = streamReader(info, Consumer.from(consumerGroup, "consumer1"), key);
            reader1.setAckPolicy(AckPolicy.MANUAL);
            StreamItemReader<String, String> reader2 = streamReader(info, Consumer.from(consumerGroup, "consumer2"), key);
            reader2.setAckPolicy(AckPolicy.MANUAL);
            ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
            TestInfo testInfo1 = new SimpleTestInfo(info, key, "1");
            TaskletStep step1 = step(testInfo1, reader1, writer1).build();
            TestInfo testInfo2 = new SimpleTestInfo(info, key, "2");
            ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
            TaskletStep step2 = step(testInfo2, reader2, writer2).build();
            SimpleFlow flow1 = flow("flow1").start(step1).build();
            SimpleFlow flow2 = flow("flow2").start(step2).build();
            SimpleFlow flow = flow("replicate").split(new SimpleAsyncTaskExecutor()).add(flow1, flow2).build();
            run(job(testInfo1).start(flow).build().build());
            Assertions.assertEquals(count, writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
            assertMessageBody(writer1.getWrittenItems());
            assertMessageBody(writer2.getWrittenItems());
            Assertions.assertEquals(count, redisCommands.xpending(key, consumerGroup).getCount());
            reader1 = streamReader(info, Consumer.from(consumerGroup, "consumer1"), key);
            reader1.setAckPolicy(AckPolicy.MANUAL);
            reader1.open(new ExecutionContext());
            reader1.ack(writer1.getWrittenItems());
            reader1.close();
            reader2 = streamReader(info, Consumer.from(consumerGroup, "consumer2"), key);
            reader2.setAckPolicy(AckPolicy.MANUAL);
            reader2.open(new ExecutionContext());
            reader2.ack(writer2.getWrittenItems());
            reader2.close();
            Assertions.assertEquals(0, redisCommands.xpending(key, consumerGroup).getCount());
        }
    }

    private static FlowBuilder<SimpleFlow> flow(String name) {
        return new FlowBuilder<>(name);
    }

    @Test
    void writeHash(TestInfo info) throws Exception {
        int count = 100;
        List<Map<String, String>> maps = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("id", String.valueOf(index));
            body.put("field1", "value1");
            body.put("field2", "value2");
            maps.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
        Hset<String, String, Map<String, String>> hset = new Hset<>(m -> "hash:" + m.remove("id"), Function.identity());
        RedisItemWriter<String, String, Map<String, String>> writer = writer(hset);
        run(info, reader, writer);
        assertEquals(count, keyCount("hash:*"));
        for (int index = 0; index < maps.size(); index++) {
            Map<String, String> hash = redisCommands.hgetall("hash:" + index);
            assertEquals(maps.get(index), hash);
        }
    }

    @Test
    void writeHashDel(TestInfo info) throws Exception {
        List<Entry<String, Map<String, String>>> hashes = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            String key = String.valueOf(index);
            Map<String, String> value = new HashMap<>();
            value.put("field1", "value1");
            redisCommands.hset("hash:" + key, value);
            Map<String, String> body = new HashMap<>();
            body.put("field2", "value2");
            hashes.add(new AbstractMap.SimpleEntry<>(key, index < 50 ? null : body));
        }
        ListItemReader<Map.Entry<String, Map<String, String>>> reader = new ListItemReader<>(hashes);
        Hset<String, String, Entry<String, Map<String, String>>> hset = new Hset<>(e -> "hash:" + e.getKey(), Entry::getValue);
        RedisItemWriter<String, String, Entry<String, Map<String, String>>> writer = writer(hset);
        run(info, reader, writer);
        assertEquals(100, keyCount("hash:*"));
        assertEquals(2, redisCommands.hgetall("hash:50").size());
    }

    @Test
    void writeDel(TestInfo info) throws Exception {
        generate(info, generator(73));
        GeneratorItemReader gen = generator(73);
        Del<String, String, KeyValue<String>> del = new Del<>(KeyValue::getKey);
        RedisItemWriter<String, String, KeyValue<String>> writer = writer(del);
        run(info, gen, writer);
        assertEquals(0, keyCount(GeneratorItemReader.DEFAULT_KEYSPACE + "*"));
    }

    @Test
    void writeLpush(TestInfo info) throws Exception {
        int count = 73;
        GeneratorItemReader gen = generator(count, ItemType.STRING);
        Lpush<String, String, KeyValue<String>> lpush = new Lpush<>(KeyValue::getKey,
                v -> Arrays.asList((String) v.getValue()));
        RedisItemWriter<String, String, KeyValue<String>> writer = writer(lpush);
        run(info, gen, writer);
        assertEquals(count, redisCommands.dbsize());
        for (String key : redisCommands.keys("*")) {
            assertEquals(KeyValue.TYPE_LIST, redisCommands.type(key));
        }
    }

    @Test
    void writeRpush(TestInfo info) throws Exception {
        int count = 73;
        GeneratorItemReader gen = generator(count, ItemType.STRING);
        Rpush<String, String, KeyValue<String>> rpush = new Rpush<>(KeyValue::getKey,
                v -> Arrays.asList((String) v.getValue()));
        RedisItemWriter<String, String, KeyValue<String>> writer = writer(rpush);
        run(info, gen, writer);
        assertEquals(count, redisCommands.dbsize());
        for (String key : redisCommands.keys("*")) {
            assertEquals(KeyValue.TYPE_LIST, redisCommands.type(key));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void writeLpushAll(TestInfo info) throws Exception {
        int count = 73;
        GeneratorItemReader gen = generator(count, ItemType.LIST);
        Lpush<String, String, KeyValue<String>> lpush = new Lpush<>(KeyValue::getKey, v -> (Collection<String>) v.getValue());
        RedisItemWriter<String, String, KeyValue<String>> writer = writer(lpush);
        run(info, gen, writer);
        assertEquals(count, redisCommands.dbsize());
        for (String key : redisCommands.keys("*")) {
            assertEquals(KeyValue.TYPE_LIST, redisCommands.type(key));
        }
    }

    @Test
    void writeExpire(TestInfo info) throws Exception {
        int count = 73;
        GeneratorItemReader gen = generator(count, ItemType.STRING);
        Expire<String, String, KeyValue<String>> expire = new Expire<>(KeyValue::getKey);
        expire.setTtl(1);
        RedisItemWriter<String, String, KeyValue<String>> writer = writer(expire);
        run(info, gen, writer);
        awaitUntil(() -> redisCommands.dbsize() == 0);
        assertEquals(0, redisCommands.dbsize());
    }

    @Test
    void writeExpireAt(TestInfo info) throws Exception {
        int count = 73;
        GeneratorItemReader gen = generator(count, ItemType.STRING);
        ExpireAt<String, String, KeyValue<String>> expire = new ExpireAt<>(KeyValue::getKey);
        expire.setTimestamp(System.currentTimeMillis());
        RedisItemWriter<String, String, KeyValue<String>> writer = writer(expire);
        run(info, gen, writer);
        awaitUntil(() -> redisCommands.dbsize() == 0);
        assertEquals(0, redisCommands.dbsize());
    }

    @Test
    void writeZset(TestInfo info) throws Exception {
        String key = "zadd";
        List<ScoredValue<String>> values = IntStream.range(0, 100)
                .mapToObj(index -> ScoredValue.just(index % 10, String.valueOf(index))).collect(Collectors.toList());
        ListItemReader<ScoredValue<String>> reader = new ListItemReader<>(values);
        Zadd<String, String, ScoredValue<String>> zadd = new Zadd<>(t -> key, t -> Arrays.asList(t));
        RedisItemWriter<String, String, ScoredValue<String>> writer = writer(zadd);
        run(info, reader, writer);
        assertEquals(1, redisCommands.dbsize());
        assertEquals(values.size(), redisCommands.zcard(key));
        assertEquals(60, redisCommands
                .zrangebyscore(key, io.lettuce.core.Range.from(Boundary.including(0), Boundary.including(5))).size());
    }

    @Test
    void writeSet(TestInfo info) throws Exception {
        String key = "sadd";
        List<String> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(String.valueOf(index));
        }
        ListItemReader<String> reader = new ListItemReader<>(values);
        Sadd<String, String, String> sadd = new Sadd<>(t -> key, v -> Arrays.asList(v));
        RedisItemWriter<String, String, String> writer = writer(sadd);
        run(info, reader, writer);
        assertEquals(1, redisCommands.dbsize());
        assertEquals(values.size(), redisCommands.scard(key));
    }

    @Test
    void writeStream(TestInfo info) throws Exception {
        String stream = "stream:0";
        List<Map<String, String>> messages = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("field1", "value1");
            body.put("field2", "value2");
            messages.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(messages);
        Xadd<String, String, Map<String, String>> xadd = new Xadd<>(t -> stream,
                m -> Arrays.asList(new StreamMessage<>(null, null, m)));
        xadd.setArgs(null);
        RedisItemWriter<String, String, Map<String, String>> writer = writer(xadd);
        run(info, reader, writer);
        Assertions.assertEquals(messages.size(), redisCommands.xlen(stream));
        List<StreamMessage<String, String>> xrange = redisCommands.xrange(stream, io.lettuce.core.Range.create("-", "+"));
        for (int index = 0; index < xrange.size(); index++) {
            StreamMessage<String, String> message = xrange.get(index);
            Assertions.assertEquals(messages.get(index), message.getBody());
        }
    }

    private MapOptions hashOptions(Range fieldCount) {
        MapOptions options = new MapOptions();
        options.setFieldCount(fieldCount);
        return options;
    }

    @Test
    void writeStructOverwrite(TestInfo info) throws Exception {
        GeneratorItemReader gen1 = generator(100, ItemType.HASH);
        gen1.setHashOptions(hashOptions(new Range(5, 5)));
        generate(info, gen1);
        GeneratorItemReader gen2 = generator(100, ItemType.HASH);
        gen2.setHashOptions(hashOptions(new Range(10, 10)));
        generate(testInfo(info, "target"), gen2);
        replicate(info, scanStructReader(), structWriter());
        assertEquals(redisCommands.hgetall("gen:1"), targetRedisCommands.hgetall("gen:1"));
    }

    @Test
    void writeStructMerge(TestInfo info) throws Exception {
        GeneratorItemReader gen1 = generator(100, ItemType.HASH);
        gen1.setHashOptions(hashOptions(new Range(5, 5)));
        generate(info, gen1);
        GeneratorItemReader gen2 = generator(100, ItemType.HASH);
        gen2.setHashOptions(hashOptions(new Range(10, 10)));
        generate(testInfo(info, "target"), gen2);
        RedisScanItemReader<String, String> reader = scanStructReader();
        RedisItemWriter<String, String, KeyValue<String>> writer = structWriter();
        writer.setMode(WriteMode.MERGE);
        run(testInfo(info, "replicate"), reader, writer);
        Map<String, String> actual = targetRedisCommands.hgetall("gen:1");
        assertEquals(10, actual.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    void writeMultiOperation(TestInfo info) throws Exception {
        int count = 100;
        List<Map<String, String>> maps = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            Map<String, String> body = new HashMap<>();
            body.put("id", String.valueOf(index));
            body.put("field1", "value1");
            body.put("field2", "value2");
            maps.add(body);
        }
        ListItemReader<Map<String, String>> reader = new ListItemReader<>(maps);
        Hset<String, String, Map<String, String>> hset = new Hset<>(m -> "hash:" + m.get("id"), Function.identity());
        Hset<String, String, Map<String, String>> hset2 = new Hset<>(m -> "hash2:" + m.get("id"), Function.identity());
        RedisItemWriter<String, String, Map<String, String>> writer = writer(new MultiOperation<>(hset, hset2));
        run(info, reader, writer);
        assertEquals(count, keyCount("hash:*"));
        assertEquals(count, keyCount("hash2:*"));
        for (int index = 0; index < maps.size(); index++) {
            assertEquals(maps.get(index), redisCommands.hgetall("hash:" + index));
            assertEquals(maps.get(index), redisCommands.hgetall("hash2:" + index));
        }
    }

    @Test
    void compareStreams(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(10);
        gen.setTypes(ItemType.STREAM);
        generate(info, gen);
        replicate(info, scanStructReader(), structWriter());
        List<KeyComparison<String>> stats = compare(info);
        Assertions.assertEquals(0, stats.stream().filter(s -> s.getStatus() != Status.OK).count());
    }

    @Test
    void compareStatus(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(120);
        generate(info, gen);
        assertDbNotEmpty(redisCommands);
        replicate(info, scanDumpReader(), dumpWriter());
        assertDbNotEmpty(targetRedisCommands);
        long deleted = 0;
        for (int index = 0; index < 13; index++) {
            deleted += targetRedisCommands.del(targetRedisCommands.randomkey());
        }
        Set<String> ttlChanges = new HashSet<>();
        for (int index = 0; index < 23; index++) {
            String key = targetRedisCommands.randomkey();
            if (key == null) {
                continue;
            }
            long ttl = targetRedisCommands.ttl(key) + 12345;
            if (targetRedisCommands.expire(key, ttl)) {
                ttlChanges.add(key);
            }
        }
        Set<String> typeChanges = new HashSet<>();
        Set<String> valueChanges = new HashSet<>();
        for (int index = 0; index < 17; index++) {
            assertDbNotEmpty(targetRedisCommands);
            String key;
            do {
                key = targetRedisCommands.randomkey();
            } while (key == null);
            String type = targetRedisCommands.type(key);
            if (KeyValue.TYPE_STRING.equals(type)) {
                if (!typeChanges.contains(key)) {
                    valueChanges.add(key);
                }
                ttlChanges.remove(key);
            } else {
                typeChanges.add(key);
                valueChanges.remove(key);
                ttlChanges.remove(key);
            }
            targetRedisCommands.set(key, "blah");
        }
        List<KeyComparison<String>> comparisons = compare(info);
        long sourceCount = redisCommands.dbsize();
        assertEquals(sourceCount, comparisons.size());
        assertEquals(sourceCount, targetRedisCommands.dbsize() + deleted);
        List<KeyComparison<String>> actualTypeChanges = comparisons.stream().filter(c -> c.getStatus() == Status.TYPE)
                .collect(Collectors.toList());
        assertEquals(typeChanges.size(), actualTypeChanges.size());
        assertEquals(valueChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.VALUE).count());
        assertEquals(ttlChanges.size(), comparisons.stream().filter(c -> c.getStatus() == Status.TTL).count());
        assertEquals(deleted, comparisons.stream().filter(c -> c.getStatus() == Status.MISSING).count());
    }

    // TODO
    // @Test
    // void readerMetrics(TestInfo info) throws Exception {
    // int count = 73;
    // generate(info, generator(count));
    // SimpleConfig registryConfig = new SimpleConfig() {
    //
    // @Override
    // public String get(String key) {
    // return null;
    // }
    //
    // @Override
    // public Duration step() {
    // return Duration.ofMillis(1);
    // }
    //
    // };
    // SimpleMeterRegistry registry = new SimpleMeterRegistry(registryConfig, Clock.SYSTEM);
    // RedisItemReader<String, String> reader = scanStructReader();
    // reader.setMeterRegistry(registry);
    // reader.open(new ExecutionContext());
    // Gauge capacity = registry
    // .get(BatchRedisMetrics.METRICS_PREFIX + RedisItemReader.QUEUE_GAUGE_NAME + BatchRedisMetrics.CAPACITY_SUFFIX)
    // .gauge();
    // Gauge size = registry
    // .get(BatchRedisMetrics.METRICS_PREFIX + RedisItemReader.QUEUE_GAUGE_NAME + BatchRedisMetrics.SIZE_SUFFIX)
    // .gauge();
    // awaitUntil(() -> size.value() == count);
    // Assertions.assertEquals(RedisItemReader.DEFAULT_QUEUE_CAPACITY, size.value() + capacity.value());
    // Counter scanReadCount = registry.get(BatchRedisMetrics.METRICS_PREFIX + KeyScanIteratorFactory.COUNTER_NAME).counter();
    // awaitUntil(() -> scanReadCount.count() == count);
    // Timer operationTimer = registry.get(BatchRedisMetrics.METRICS_PREFIX + OperationExecutor.TIMER_NAME).timer();
    // Assertions.assertEquals(2, operationTimer.count());
    // reader.close();
    // }

    @Test
    void readTimeseries(TestInfo info) throws Exception {
        String key = "myts";
        Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1), Sample.of(System.currentTimeMillis() + 10, 2.2) };
        for (Sample sample : samples) {
            redisCommands.tsAdd(key, sample);
        }
        RedisScanItemReader<String, String> reader = scanStructReader();
        reader.open(new ExecutionContext());
        KeyValue<String> ds = reader.read();
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ItemType.TIMESERIES.getString(), ds.getType());
        Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
        reader.close();
    }

    @Test
    void readTimeseriesByteArray(TestInfo info) throws Exception {
        String key = "myts";
        Sample[] samples = { Sample.of(System.currentTimeMillis(), 1.1), Sample.of(System.currentTimeMillis() + 10, 2.2) };
        for (Sample sample : samples) {
            redisCommands.tsAdd(key, sample);
        }
        RedisScanItemReader<byte[], byte[]> reader = scanStructReader(ByteArrayCodec.INSTANCE);
        reader.open(new ExecutionContext());
        Function<String, byte[]> toByteArrayKeyFunction = BatchUtils.toByteArrayKeyFunction(StringCodec.UTF8);
        KeyValue<byte[]> ds = reader.read();
        Assertions.assertArrayEquals(toByteArrayKeyFunction.apply(key), ds.getKey());
        Assertions.assertEquals(ItemType.TIMESERIES.getString(), ds.getType());
        Assertions.assertEquals(Arrays.asList(samples), ds.getValue());
        reader.close();
    }

    @Test
    void writeTimeseries(TestInfo info) throws Exception {
        String key = "ts";
        long now = Instant.now().toEpochMilli();
        List<Sample> samples = IntStream.range(0, 100).mapToObj(index -> Sample.of(now + index, index))
                .collect(Collectors.toList());
        ListItemReader<Collection<Sample>> reader = new ListItemReader<>(Arrays.asList(samples));
        TsAdd<String, String, Collection<Sample>> tsAdd = new TsAdd<>(keyFunction(key), Function.identity());
        RedisItemWriter<String, String, Collection<Sample>> writer = writer(tsAdd);
        run(info, reader, writer);
        assertEquals(1, redisCommands.dbsize());
    }

    @SuppressWarnings("unchecked")
    @Test
    void writeTsAddAll(TestInfo info) throws Exception {
        int count = 10;
        GeneratorItemReader reader = generator(count, ItemType.TIMESERIES);
        AddOptions<String, String> addOptions = AddOptions.<String, String> builder().policy(DuplicatePolicy.LAST).build();
        TsAdd<String, String, KeyValue<String>> tsadd = new TsAdd<>(KeyValue::getKey, t -> (Collection<Sample>) t.getValue());
        tsadd.setOptions(addOptions);
        RedisItemWriter<String, String, KeyValue<String>> writer = client(RedisItemWriter.operation(tsadd));
        run(info, reader, writer);
        for (int index = 1; index <= count; index++) {
            Assertions.assertEquals(TimeSeriesOptions.DEFAULT_SAMPLE_COUNT.getMin(),
                    redisCommands.tsRange(reader.key(index), TimeRange.unbounded(), RangeOptions.builder().build()).size(), 2);
        }
    }

    @Test
    void writeTsAdd(TestInfo info) throws Exception {
        String key = "ts:1";
        Random random = new Random();
        int count = 100;
        List<Sample> samples = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            long timestamp = System.currentTimeMillis() - count + (index % (count / 2));
            samples.add(Sample.of(timestamp, random.nextDouble()));
        }
        ListItemReader<Collection<Sample>> reader = new ListItemReader<>(Arrays.asList(samples));
        AddOptions<String, String> addOptions = AddOptions.<String, String> builder().policy(DuplicatePolicy.LAST).build();
        TsAdd<String, String, Collection<Sample>> tsadd = new TsAdd<>(keyFunction(key), Function.identity());
        tsadd.setOptions(addOptions);
        RedisItemWriter<String, String, Collection<Sample>> writer = writer(tsadd);
        run(info, reader, writer);
        Assertions.assertEquals(count / 2,
                redisCommands.tsRange(key, TimeRange.unbounded(), RangeOptions.builder().build()).size(), 2);
    }

    @Test
    void writeSug(TestInfo info) throws Exception {
        String key = "sugadd";
        List<Suggestion<String>> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(Suggestion.string("word" + index).score(index + 1).payload("payload" + index).build());
        }
        ListItemReader<Suggestion<String>> reader = new ListItemReader<>(values);
        Sugadd<String, String, Suggestion<String>> sugadd = new Sugadd<>(keyFunction(key), Function.identity());
        RedisItemWriter<String, String, Suggestion<String>> writer = writer(sugadd);
        run(info, reader, writer);
        assertEquals(1, redisCommands.dbsize());
        assertEquals(values.size(), redisCommands.ftSuglen(key));
    }

    @Test
    void writeSugIncr(TestInfo info) throws Exception {
        String key = "sugaddIncr";
        List<Suggestion<String>> values = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            values.add(Suggestion.string("word" + index).score(index + 1).payload("payload" + index).build());
        }
        ListItemReader<Suggestion<String>> reader = new ListItemReader<>(values);
        Sugadd<String, String, Suggestion<String>> sugadd = new Sugadd<>(keyFunction(key), Function.identity());
        sugadd.setIncr(true);
        RedisItemWriter<String, String, Suggestion<String>> writer = writer(sugadd);
        run(info, reader, writer);
        assertEquals(1, redisCommands.dbsize());
        assertEquals(values.size(), redisCommands.ftSuglen(key));
    }

    @Test
    void replicateStruct(TestInfo info) throws Exception {
        replicate(info, scanStructReader(), structWriter());
    }

    // TODO
    // @Test
    // void replicateStructLive(TestInfo info) throws Exception {
    // RedisItemReader<String, String> reader = scanStructReader();
    // reader.setMode(ReaderMode.LIVE);
    // replicate(info, reader, structWriter());
    // }
    //
    // @Test
    // void replicateSetLiveOnly(TestInfo info) throws Exception {
    // String key = "myset";
    // redisCommands.sadd(key, "1", "2", "3", "4", "5");
    // RedisItemReader<String, String> reader = scanStructReader();
    // reader.setMode(ReaderMode.LIVEONLY);
    // reader.setEventQueueCapacity(100);
    // executeWhenSubscribers(() -> redisCommands.srem(key, "5"));
    // replicate(info, reader, structWriter());
    // assertEquals(redisCommands.smembers(key), targetRedisCommands.smembers(key));
    // }

}
