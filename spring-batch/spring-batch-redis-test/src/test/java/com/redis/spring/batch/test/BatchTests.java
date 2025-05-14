package com.redis.spring.batch.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.Beers;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.spring.batch.JobUtils;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.Wait;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.Range;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.ItemType;
import com.redis.spring.batch.item.redis.reader.*;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;
import com.redis.spring.batch.item.redis.reader.StreamItemReader.AckPolicy;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite;
import com.redis.spring.batch.item.redis.writer.impl.*;
import io.lettuce.core.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessages;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
abstract class BatchTests extends AbstractTargetTestBase {

    private void checkJobExecution(JobExecution jobExecution) throws Throwable {
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            if (JobUtils.isFailed(stepExecution.getExitStatus())) {
                throw stepExecution.getFailureExceptions().get(0);
            }
        }
        if (JobUtils.isFailed(jobExecution.getExitStatus())) {
            throw jobExecution.getFailureExceptions().get(0);
        }
    }

    @Test
    void compareSet(TestInfo info) throws Exception {
        redisCommands.sadd("set:1", "value1", "value2");
        targetRedisCommands.sadd("set:1", "value2", "value1");
        Assertions.assertEquals(Status.OK, compare(info).get(0).getStatus());
    }

    @Test
    void estimateScanSize(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(300, ItemType.HASH, ItemType.STRING);
        generate(info, gen);
        long expectedCount = redisCommands.dbsize();
        RedisScanSizeEstimator estimator = new RedisScanSizeEstimator();
        estimator.setClient(redisClient);
        estimator.setKeyPattern(GeneratorItemReader.DEFAULT_KEYSPACE + ":*");
        estimator.setSamples(300);
        assertEquals(expectedCount, estimator.getAsLong(), (float) expectedCount / 10);
        estimator.setKeyType(ItemType.HASH.getString());
        assertEquals((float) expectedCount / 2, estimator.getAsLong(), (float) expectedCount / 10);
    }

    @Test
    void compareQuick(TestInfo info) throws Exception {
        int sourceCount = 2;
        for (int index = 0; index < sourceCount; index++) {
            redisCommands.set("key:" + index, "value:" + index);
        }
        int targetCount = 1;
        for (int index = 0; index < targetCount; index++) {
            targetRedisCommands.set("key:" + index, "value:" + index);
        }
        List<KeyComparison<String>> comparisons = compare(info);
        Assertions.assertEquals(sourceCount - targetCount,
                comparisons.stream().filter(s -> s.getStatus() == Status.MISSING).count());
    }

    @Test
    void readStruct(TestInfo info) throws Exception {
        generate(info, generator(73));
        RedisScanItemReader<String, String> reader = scanStructReader();
        List<? extends KeyValue<String>> list = readAll(info, reader);
        assertEquals(redisCommands.dbsize(), list.size());
    }

    @Test
    void readStreamAutoAck(TestInfo info) throws Exception {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamAutoAck";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(info, consumer, stream);
        reader.setAckPolicy(AckPolicy.AUTO);
        reader.open(new ExecutionContext());
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        String id1 = redisCommands.xadd(stream, body);
        String id2 = redisCommands.xadd(stream, body);
        String id3 = redisCommands.xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());
        assertStreamEquals(id1, body, stream, messages.get(0));
        assertStreamEquals(id2, body, stream, messages.get(1));
        assertStreamEquals(id3, body, stream, messages.get(2));
        reader.close();
        Assertions.assertEquals(0, redisCommands.xpending(stream, consumerGroup).getCount(), "pending messages");
    }

    @Test
    void readStreamAutoAck2(TestInfo info) throws Exception {
        readStreamsAutoAck(info, 1);
    }

    @SuppressWarnings("unchecked")
    protected void readStreamsAutoAck(TestInfo info, int streamCount) throws Exception {
        GeneratorItemReader generator = generator(streamCount, ItemType.STREAM);
        generator.getStreamOptions().setMessageCount(new Range(73, 73));
        Map<String, List<StreamMessage<String, String>>> expected = readAll(info, generator).stream()
                .flatMap(t -> ((List<StreamMessage<String, String>>) t.getValue()).stream())
                .collect(Collectors.groupingBy(StreamMessage::getStream));
        String[] keys = expected.keySet().toArray(new String[0]);
        String consumerGroup = "batchtests-readStreamAutoAck";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        StreamItemReader<String, String> reader = streamReader(info, consumer, keys);
        reader.setAckPolicy(AckPolicy.AUTO);
        Map<String, List<String>> messageIds = expected.entrySet().stream().map(this::xadd)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        Map<String, List<StreamMessage<String, String>>> actual = readAllMessages(reader).stream()
                .collect(Collectors.groupingBy(StreamMessage::getStream));
        for (String stream : keys) {
            List<String> expectedMessageIds = messageIds.get(stream);
            List<StreamMessage<String, String>> expectedMessages = expected.get(stream);
            List<StreamMessage<String, String>> actualMessages = actual.get(stream);
            Assertions.assertEquals(expectedMessages.size(), actualMessages.size());
            for (int index = 0; index < expectedMessages.size(); index++) {
                Assertions.assertEquals(expectedMessages.get(index).getBody(), actualMessages.get(index).getBody());
                Assertions.assertEquals(expectedMessageIds.get(index), actualMessages.get(index).getId());
            }
        }
        reader.close();
        for (String stream : keys) {
            Assertions.assertEquals(0, redisCommands.xpending(stream, consumerGroup).getCount(), "pending messages");
        }
    }

    private Entry<String, List<String>> xadd(Entry<String, List<StreamMessage<String, String>>> entry) {
        return new SimpleEntry<>(entry.getKey(),
                entry.getValue().stream().map(m -> redisCommands.xadd(entry.getKey(), m.getBody()))
                        .collect(Collectors.toList()));
    }

    @Test
    void readStreamManualAck(TestInfo info) throws Exception {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamManualAck";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(info, consumer, stream);
        reader.setAckPolicy(AckPolicy.MANUAL);
        reader.open(new ExecutionContext());
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        String id1 = redisCommands.xadd(stream, body);
        String id2 = redisCommands.xadd(stream, body);
        String id3 = redisCommands.xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());

        assertStreamEquals(id1, body, stream, messages.get(0));
        assertStreamEquals(id2, body, stream, messages.get(1));
        assertStreamEquals(id3, body, stream, messages.get(2));
        PendingMessages pendingMsgsBeforeCommit = redisCommands.xpending(stream, consumerGroup);
        Assertions.assertEquals(3, pendingMsgsBeforeCommit.getCount(), "pending messages before commit");
        redisCommands.xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());
        PendingMessages pendingMsgsAfterCommit = redisCommands.xpending(stream, consumerGroup);
        Assertions.assertEquals(1, pendingMsgsAfterCommit.getCount(), "pending messages after commit");
        reader.close();
    }

    @Test
    void readStreamManualAckRecover(TestInfo info) throws Exception {
        String stream = "stream1";
        Consumer<String> consumer = Consumer.from("batchtests-readStreamManualAckRecover", "consumer1");
        final StreamItemReader<String, String> reader = streamReader(info, consumer, stream);
        reader.setAckPolicy(AckPolicy.MANUAL);
        reader.open(new ExecutionContext());
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        redisCommands.xadd(stream, body);
        redisCommands.xadd(stream, body);
        redisCommands.xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());

        List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
        redisCommands.xadd(stream, body);
        redisCommands.xadd(stream, body);
        redisCommands.xadd(stream, body);

        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(info, consumer, stream);
        reader2.setAckPolicy(AckPolicy.MANUAL);
        reader2.open(new ExecutionContext());

        awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));

        Assertions.assertEquals(6, recoveredMessages.size());
    }

    @Test
    void readStreamManualAckRecoverUncommitted(TestInfo info) throws Exception {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamManualAckRecoverUncommitted";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(info, consumer, stream);
        reader.setAckPolicy(AckPolicy.MANUAL);
        reader.open(new ExecutionContext());
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        redisCommands.xadd(stream, body);
        redisCommands.xadd(stream, body);
        String id3 = redisCommands.xadd(stream, body);
        List<StreamMessage<String, String>> messages = new ArrayList<>();
        awaitUntil(() -> messages.addAll(reader.readMessages()));
        Assertions.assertEquals(3, messages.size());
        redisCommands.xack(stream, consumerGroup, messages.get(0).getId(), messages.get(1).getId());

        List<StreamMessage<String, String>> recoveredMessages = new ArrayList<>();
        String id4 = redisCommands.xadd(stream, body);
        String id5 = redisCommands.xadd(stream, body);
        String id6 = redisCommands.xadd(stream, body);
        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(info, consumer, stream);
        reader2.setAckPolicy(AckPolicy.MANUAL);
        reader2.setOffset(stream, messages.get(1).getId());
        reader2.open(new ExecutionContext());

        // Wait until task.poll() doesn't return any more records
        awaitUntil(() -> recoveredMessages.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredMessages.addAll(reader2.readMessages()));
        List<String> recoveredIds = recoveredMessages.stream().map(StreamMessage::getId).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.<String> asList(id3, id4, id5, id6), recoveredIds, "recoveredIds");
        reader2.close();
    }

    @Test
    void readStreamManualAckRecoverFromOffset(TestInfo info) throws Exception {
        String stream = "stream1";
        String consumerGroup = "batchtests-readStreamManualAckRecoverFromOffset";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(info, consumer, stream);
        reader.setAckPolicy(AckPolicy.MANUAL);
        reader.open(new ExecutionContext());
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        redisCommands.xadd(stream, body);
        redisCommands.xadd(stream, body);
        String id3 = redisCommands.xadd(stream, body);
        List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
        awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
        Assertions.assertEquals(3, sourceRecords.size());

        List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
        String id4 = redisCommands.xadd(stream, body);
        String id5 = redisCommands.xadd(stream, body);
        String id6 = redisCommands.xadd(stream, body);

        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(info, consumer, stream);
        reader2.setAckPolicy(AckPolicy.MANUAL);
        reader2.setOffset(stream, id3);
        reader2.open(new ExecutionContext());

        // Wait until task.poll() doesn't return any more records
        awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
        List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.<String> asList(id4, id5, id6), recoveredIds, "recoveredIds");
        reader2.close();
    }

    @Test
    void readStreamRecoverManualAckToAutoAck(TestInfo info) throws Exception {
        String stream = "stream1";
        String consumerGroup = "readStreamRecoverManualAckToAutoAck";
        Consumer<String> consumer = Consumer.from(consumerGroup, "consumer1");
        final StreamItemReader<String, String> reader = streamReader(info, consumer, stream);
        reader.setAckPolicy(AckPolicy.MANUAL);
        reader.open(new ExecutionContext());
        String field1 = "field1";
        String value1 = "value1";
        String field2 = "field2";
        String value2 = "value2";
        Map<String, String> body = map(field1, value1, field2, value2);
        redisCommands.xadd(stream, body);
        redisCommands.xadd(stream, body);
        redisCommands.xadd(stream, body);
        List<StreamMessage<String, String>> sourceRecords = new ArrayList<>();
        awaitUntil(() -> sourceRecords.addAll(reader.readMessages()));
        Assertions.assertEquals(3, sourceRecords.size());

        List<StreamMessage<String, String>> recoveredRecords = new ArrayList<>();
        String id4 = redisCommands.xadd(stream, body);
        String id5 = redisCommands.xadd(stream, body);
        String id6 = redisCommands.xadd(stream, body);
        reader.close();

        final StreamItemReader<String, String> reader2 = streamReader(info, consumer, stream);
        reader2.setAckPolicy(AckPolicy.AUTO);
        reader2.open(new ExecutionContext());

        // Wait until task.poll() doesn't return any more records
        awaitUntil(() -> recoveredRecords.addAll(reader2.readMessages()));
        awaitUntil(() -> !recoveredRecords.addAll(reader2.readMessages()));
        List<String> recoveredIds = recoveredRecords.stream().map(StreamMessage::getId).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(id4, id5, id6), recoveredIds, "recoveredIds");

        PendingMessages pending = redisCommands.xpending(stream, consumerGroup);
        Assertions.assertEquals(0, pending.getCount(), "pending message count");
        reader2.close();
    }

    @Test
    void readStreamAck(TestInfo info) throws Exception {
        readStreamsAck(info, 1);
    }

    protected void readStreamsAck(TestInfo info, int streamCount) throws Exception {
        generateStreams(info, streamCount, 57);
        List<String> keys = ScanIterator.scan(redisCommands, KeyScanArgs.Builder.type(ItemType.STREAM.getString())).stream()
                .collect(Collectors.toList());
        Consumer<String> consumer = Consumer.from("batchtests-readmessages", "consumer1");
        for (String key : keys) {
            long count = redisCommands.xlen(key);
            StreamItemReader<String, String> reader = streamReader(info, consumer, key);
            List<StreamMessage<String, String>> messages = readAllMessages(reader);
            assertEquals(count, messages.size());
            assertMessageBody(messages);
            reader.ack(reader.readMessages());
            reader.close();
        }
    }

    private <K, V> List<StreamMessage<K, V>> readAllMessages(StreamItemReader<K, V> reader) throws Exception {
        reader.open(new ExecutionContext());
        List<StreamMessage<K, V>> messages = new ArrayList<>();
        StreamMessage<K, V> message;
        while ((message = reader.read()) != null) {
            messages.add(message);
        }
        return messages;
    }

    @Test
    void readStream(TestInfo info) throws Exception {
        readStreams(info, 1);
    }

    protected void readStreams(TestInfo info, int streamCount) throws Exception {
        generateStreams(info, streamCount, 73);
        KeyScanArgs args = KeyScanArgs.Builder.type(ItemType.STREAM.getString());
        List<String> keys = ScanIterator.scan(redisCommands, args).stream().collect(Collectors.toList());
        long count = keys.stream().mapToLong(k -> redisCommands.xlen(k)).sum();
        StreamItemReader<String, String> reader = streamReader(info, null, keys.toArray(new String[0]));
        List<StreamMessage<String, String>> messages = readAllMessages(reader);
        reader.close();
        Assertions.assertEquals(count, messages.size());
        assertMessageBody(messages);
    }

    @Test
    void readStreamConsumer(TestInfo info) throws Exception {
        readStreamsConsumer(info, 1);
    }

    protected void readStreamsConsumer(TestInfo info, int streamCount) throws Exception {
        generateStreams(info, streamCount, 73);
        List<String> keys = ScanIterator.scan(redisCommands, KeyScanArgs.Builder.type(ItemType.STREAM.getString())).stream()
                .collect(Collectors.toList());
        Consumer<String> consumer = Consumer.from("batchtests-readstreamjob", "consumer1");
        for (String key : keys) {
            long count = redisCommands.xlen(key);
            StreamItemReader<String, String> reader = streamReader(info, consumer, key);
            List<StreamMessage<String, String>> messages = readAllMessages(reader);
            reader.close();
            Assertions.assertEquals(count, messages.size());
            assertMessageBody(messages);
        }
    }

    @Test
    void readStructHash(TestInfo info) throws Exception {
        String key = "myhash";
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value1");
        hash.put("field2", "value2");
        redisCommands.hset(key, hash);
        long ttl = System.currentTimeMillis() + 123456;
        redisCommands.pexpireat(key, ttl);
        RedisScanItemReader<String, String> reader = scanStructReader();
        reader.open(new ExecutionContext());
        KeyValue<String> keyValue = reader.read();
        Assertions.assertEquals(key, keyValue.getKey());
        assertTtlEquals(ttl, keyValue.getTtl());
        Assertions.assertEquals(KeyValue.TYPE_HASH, keyValue.getType());
        Assertions.assertEquals(hash, keyValue.getValue());
        reader.close();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void readStructZset(TestInfo info) throws Exception {
        String key = "myzset";
        ScoredValue[] values = { ScoredValue.just(123.456, "value1"), ScoredValue.just(654.321, "value2") };
        redisCommands.zadd(key, values);
        RedisScanItemReader<String, String> reader = scanStructReader();
        reader.open(new ExecutionContext());
        KeyValue<String> ds = reader.read();
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ItemType.ZSET.getString(), ds.getType());
        Assertions.assertEquals(new HashSet<>(Arrays.asList(values)), ds.getValue());
        reader.close();
    }

    @Test
    void readStructList() throws Exception {
        String key = "mylist";
        List<String> values = Arrays.asList("value1", "value2");
        redisCommands.rpush(key, values.toArray(new String[0]));
        RedisScanItemReader<String, String> reader = scanStructReader();
        reader.open(new ExecutionContext());
        KeyValue<String> ds = reader.read();
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ItemType.LIST.getString(), ds.getType());
        Assertions.assertEquals(values, ds.getValue());
        reader.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void readStructStream() throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        redisCommands.xadd(key, body);
        redisCommands.xadd(key, body);
        RedisScanItemReader<String, String> reader = scanStructReader();
        reader.open(new ExecutionContext());
        KeyValue<String> ds = reader.read();
        Assertions.assertEquals(key, ds.getKey());
        Assertions.assertEquals(ItemType.STREAM.getString(), ds.getType());
        List<StreamMessage<String, String>> messages = (List<StreamMessage<String, String>>) ds.getValue();
        Assertions.assertEquals(2, messages.size());
        for (StreamMessage<String, String> message : messages) {
            Assertions.assertEquals(body, message.getBody());
            Assertions.assertNotNull(message.getId());
        }
        reader.close();
    }

    @Test
    void readDumpStream(TestInfo info) throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        redisCommands.xadd(key, body);
        redisCommands.xadd(key, body);
        long ttl = System.currentTimeMillis() + 123456;
        redisCommands.pexpireat(key, ttl);
        RedisScanItemReader<byte[], byte[]> reader = scanDumpReader();
        reader.open(new ExecutionContext());
        KeyValue<byte[]> dump = reader.read();
        Assertions.assertArrayEquals(toByteArray(key), dump.getKey());
        assertTtlEquals(ttl, dump.getTtl());
        redisCommands.del(key);
        redisCommands.restore(key, (byte[]) dump.getValue(), RestoreArgs.Builder.ttl(ttl).absttl());
        Assertions.assertEquals(ItemType.STREAM.getString(), redisCommands.type(key));
        reader.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void readStructStreamByteArray() throws Exception {
        String key = "mystream";
        Map<String, String> body = new HashMap<>();
        body.put("field1", "value1");
        body.put("field2", "value2");
        redisCommands.xadd(key, body);
        redisCommands.xadd(key, body);
        RedisScanItemReader<byte[], byte[]> reader = client(RedisScanItemReader.struct(ByteArrayCodec.INSTANCE));
        reader.open(new ExecutionContext());
        KeyValue<byte[]> ds = reader.read();
        Assertions.assertArrayEquals(toByteArray(key), ds.getKey());
        Assertions.assertEquals(ItemType.STREAM.getString(), ds.getType());
        List<StreamMessage<byte[], byte[]>> messages = (List<StreamMessage<byte[], byte[]>>) ds.getValue();
        Assertions.assertEquals(2, messages.size());
        for (StreamMessage<byte[], byte[]> message : messages) {
            Map<byte[], byte[]> actual = message.getBody();
            Assertions.assertEquals(2, actual.size());
            Map<String, String> actualString = new HashMap<>();
            actual.forEach((k, v) -> actualString.put(toString(k), toString(v)));
            Assertions.assertEquals(body, actualString);
        }
        reader.close();
    }

    @Test
    void readStructHLL(TestInfo info) throws Exception {
        String key1 = "hll:1";
        redisCommands.pfadd(key1, "member:1", "member:2");
        String key2 = "hll:2";
        redisCommands.pfadd(key2, "member:1", "member:2", "member:3");
        RedisScanItemReader<String, String> reader = scanStructReader();
        List<? extends KeyValue<String>> items = readAll(info, reader);
        Assertions.assertEquals(2, items.size());
        Optional<? extends KeyValue<String>> result = items.stream().filter(ds -> ds.getKey().equals(key1)).findFirst();
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(key1, result.get().getKey());
        Assertions.assertEquals(ItemType.STRING.getString(), result.get().getType());
        Assertions.assertEquals(redisCommands.get(key1), result.get().getValue());
    }

    public static <T> Function<T, String> keyFunction(String key) {
        return t -> key;
    }

    @Test
    void writeGeo(TestInfo info) throws Exception {
        ListItemReader<Geo> reader = new ListItemReader<>(Arrays.asList(new Geo("Venice Breakwater", -118.476056, 33.985728),
                new Geo("Long Beach National", -73.667022, 40.582739)));
        Geoadd<String, String, Geo> geoadd = new Geoadd<>(keyFunction("geoset"), this::geoValue);
        RedisItemWriter<String, String, Geo> writer = writer(geoadd);
        run(info, reader, writer);
        Set<String> radius1 = redisCommands.georadius("geoset", -118, 34, 100, GeoArgs.Unit.mi);
        assertEquals(1, radius1.size());
        assertTrue(radius1.contains("Venice Breakwater"));
    }

    @Test
    void writeDel(TestInfo info) throws Exception {
        int count = 123;
        List<String> keys = IntStream.range(0, count).mapToObj(i -> "key:" + i).collect(Collectors.toList());
        keys.forEach(k -> redisCommands.set(k, k));
        Assertions.assertEquals(count, redisCommands.dbsize());
        ListItemReader<String> reader = new ListItemReader<>(keys);
        RedisItemWriter<String, String, String> writer = writer(new Del<>(Function.identity()));
        run(info, reader, writer);
        Assertions.assertEquals(0, redisCommands.dbsize());
    }

    private Collection<GeoValue<String>> geoValue(Geo geo) {
        return Arrays.asList(GeoValue.just(geo.getLongitude(), geo.getLatitude(), geo.getMember()));
    }

    private static class Geo {

        private String member;

        private double longitude;

        private double latitude;

        public Geo(String member, double longitude, double latitude) {
            this.member = member;
            this.longitude = longitude;
            this.latitude = latitude;
        }

        public String getMember() {
            return member;
        }

        public double getLongitude() {
            return longitude;
        }

        public double getLatitude() {
            return latitude;
        }

    }

    @SuppressWarnings("unchecked")
    @Test
    void writeWait(TestInfo info) throws Exception {
        List<KeyValue<String>> items = IntStream.range(0, 5).mapToObj(index -> {
            KeyValue<String> item = new KeyValue<>();
            item.setKey("key:" + index);
            item.setValue(map("field1", "value1", "field2", "value2"));
            return item;
        }).collect(Collectors.toList());
        ListItemReader<KeyValue<String>> reader = new ListItemReader<>(items);
        Hset<String, String, KeyValue<String>> hset = new Hset<>(KeyValue::getKey, v -> (Map<String, String>) v.getValue());
        RedisItemWriter<String, String, KeyValue<String>> writer = writer(hset);
        writer.setWait(Wait.of(1, Duration.ofMillis(300)));
        try {
            run(job(info).start(step(info, reader, writer).build()).build());
            Assertions.fail("No exception thrown");
        } catch (JobExecutionException e) {
            assertEquals("Insufficient replication level (0/1)", e.getCause().getCause().getMessage());
        }
    }

    @Test
    void readKeyEventsDedupe() throws Exception {
        enableKeyspaceNotifications();
        KeyEventItemReader<String, String> keyReader = new KeyEventItemReader<>(redisClient, StringCodec.UTF8);
        keyReader.open(new ExecutionContext());
        try {
            String key = "key1";
            redisCommands.zadd(key, 1, "member1");
            redisCommands.zadd(key, 2, "member2");
            redisCommands.zadd(key, 3, "member3");
            awaitUntil(() -> keyReader.getQueue().size() == 1);
            Assertions.assertEquals(key, keyReader.getQueue().take().getKey());
        } finally {
            keyReader.close();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void compareBinaryKeyValue(TestInfo info) throws Exception {
        byte[] zsetKey = randomBytes();
        Collection<ScoredValue<byte[]>> zsetValue = new ArrayList<>();
        for (int index = 0; index < 10; index++) {
            zsetValue.add(ScoredValue.just(index, randomBytes()));
        }
        byte[] listKey = randomBytes();
        List<byte[]> listValue = new ArrayList<>();
        for (int index = 0; index < 10; index++) {
            listValue.add(randomBytes());
        }
        byte[] setKey = randomBytes();
        Set<byte[]> setValue = new HashSet<>();
        for (int index = 0; index < 10; index++) {
            setValue.add(randomBytes());
        }
        byte[] hashKey = randomBytes();
        Map<byte[], byte[]> hashValue = byteArrayMap();
        try (StatefulRedisModulesConnection<byte[], byte[]> connection = ConnectionBuilder.client(redisClient)
                .connection(ByteArrayCodec.INSTANCE)) {
            RedisModulesCommands<byte[], byte[]> source = connection.sync();
            try (StatefulRedisModulesConnection<byte[], byte[]> targetConnection = targetRedisConnection(
                    ByteArrayCodec.INSTANCE)) {
                RedisModulesCommands<byte[], byte[]> target = targetConnection.sync();
                source.sadd(setKey, setValue.toArray(new byte[0][]));
                target.sadd(setKey, setValue.toArray(new byte[0][]));
                source.hset(hashKey, hashValue);
                target.hset(hashKey, hashValue);
                source.lpush(listKey, listValue.toArray(new byte[0][]));
                target.lpush(listKey, listValue.toArray(new byte[0][]));
                source.zadd(zsetKey, zsetValue.toArray(new ScoredValue[0]));
                target.zadd(zsetKey, zsetValue.toArray(new ScoredValue[0]));
                byte[] streamKey = randomBytes();
                for (int index = 0; index < 10; index++) {
                    Map<byte[], byte[]> body = byteArrayMap();
                    String id = source.xadd(streamKey, body);
                    XAddArgs args = new XAddArgs();
                    args.id(id);
                    target.xadd(streamKey, args, body);
                }
            }
        }
        List<KeyComparison<byte[]>> stats = compare(info, ByteArrayCodec.INSTANCE);
        Assertions.assertFalse(stats.isEmpty());
        Assertions.assertEquals(0, stats.stream().filter(s -> s.getStatus() != Status.OK).count());
    }

    private Map<byte[], byte[]> byteArrayMap() {
        Map<byte[], byte[]> hash = new HashMap<>();
        int fieldCount = 10;
        for (int index = 0; index < fieldCount; index++) {
            hash.put(randomBytes(), randomBytes());
        }
        return hash;

    }

    private final Random random = new Random();

    private byte[] randomBytes() {
        byte[] bytes = new byte[10];
        random.nextBytes(bytes);
        return bytes;
    }

    private static final String JSON_BEER_1 = "[{\"id\":\"1\",\"brewery_id\":\"812\",\"name\":\"Hocus Pocus\",\"abv\":\"4.5\",\"ibu\":\"0\",\"srm\":\"0\",\"upc\":\"0\",\"filepath\":\"\",\"descript\":\"Our take on a classic summer ale.  A toast to weeds, rays, and summer haze.  A light, crisp ale for mowing lawns, hitting lazy fly balls, and communing with nature, Hocus Pocus is offered up as a summer sacrifice to clodless days.\\n\\nIts malty sweetness finishes tart and crisp and is best apprediated with a wedge of orange.\",\"add_user\":\"0\",\"last_mod\":\"2010-07-22 20:00:20 UTC\",\"style_name\":\"Light American Wheat Ale or Lager\",\"cat_name\":\"Other Style\"}]";

    private static final int BEER_COUNT = 1019;

    @Test
    void beerIndex() throws Exception {
        Beers.populateIndex(redisConnection);
        IndexInfo indexInfo = IndexInfo.parse(redisCommands.ftInfo(Beers.INDEX));
        Assertions.assertEquals(BEER_COUNT, indexInfo.getNumDocs());
    }

    @Test
    void writeJsonSet(TestInfo info) throws Exception {
        JsonSet<String, String, JsonNode> jsonSet = new JsonSet<>(n -> "beer:" + n.get("id").asText(), JsonNode::toString);
        RedisItemWriter<String, String, JsonNode> writer = writer(jsonSet);
        IteratorItemReader<JsonNode> reader = new IteratorItemReader<>(Beers.jsonNodeIterator());
        run(info, reader, writer);
        Assertions.assertEquals(BEER_COUNT, keyCount("beer:*"));
        Assertions.assertEquals(new ObjectMapper().readTree(JSON_BEER_1),
                new ObjectMapper().readTree(redisCommands.jsonGet("beer:1").toString()));
    }

    @Test
    void writeConditionalDel(TestInfo info) throws Throwable {
        generate(info, generator(100));
        GeneratorItemReader reader = generator(100, ItemType.HASH);
        Function<KeyValue<String>, Boolean> classifier = t -> t.getKey().endsWith("3");
        ClassifierOperation<String, String, KeyValue<String>, Boolean> operation = new ClassifierOperation<>(classifier);
        operation.setOperation(Boolean.FALSE, new KeyValueWrite<>());
        operation.setOperation(Boolean.TRUE, new Del<>(KeyValue::getKey));
        RedisItemWriter<String, String, KeyValue<String>> writer = client(RedisItemWriter.operation(operation));
        checkJobExecution(run(info, reader, writer));
        List<? extends KeyValue<String>> items = readAll(info, reader);
        for (KeyValue<String> item : items) {
            if (classifier.apply(item)) {
                Assertions.assertEquals(0, redisCommands.exists(item.getKey()));
            } else {
                Assertions.assertEquals(ItemType.HASH.getString(), redisCommands.type(item.getKey()));
            }
        }
    }

    @Test
    void writeJsonDel(TestInfo info) throws Exception {
        GeneratorItemReader gen = generator(73, ItemType.JSON);
        generate(info, gen);
        JsonDel<String, String, KeyValue<String>> jsonDel = new JsonDel<>(KeyValue::getKey);
        RedisItemWriter<String, String, KeyValue<String>> writer = writer(jsonDel);
        run(info, gen, writer);
        Assertions.assertEquals(0, redisCommands.dbsize());
    }

    @Test
    void replicateDump(TestInfo info) throws Exception {
        replicate(info, scanDumpReader(), dumpWriter());
    }

    @Test
    void replicateLiveDump(TestInfo info) throws Exception {
        replicate(info, client(RedisLiveItemReader.dump()), dumpWriter());
    }

    @Test
    void replicateLiveStruct(TestInfo info) throws Exception {
        replicate(info, client(RedisLiveItemReader.struct()), structWriter());
    }

}
