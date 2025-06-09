package com.redis.spring.batch.item.redis.reader;

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.redis.batch.KeyEvent;
import com.redis.batch.operation.KeyValueRead;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.batch.KeyValue;
import com.redis.batch.RedisOperation;

import io.lettuce.core.codec.RedisCodec;
import lombok.ToString;

@ToString
public class RedisLiveItemReader<K, V> extends RedisItemReader<K, V> implements PollableItemReader<KeyValue<K>> {

    public static final int DEFAULT_QUEUE_CAPACITY = KeyEventItemReader.DEFAULT_QUEUE_CAPACITY;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private int database;

    private KeyEventItemReader<K, V> keyEventReader;

    private Iterator<KeyValue<K>> iterator = Collections.emptyIterator();

    ;

    public RedisLiveItemReader(RedisCodec<K, V> codec, RedisOperation<K, V, K, KeyValue<K>> operation) {
        super(codec, operation);
    }

    public KeyEventItemReader<K, V> getKeyEventReader() {
        return keyEventReader;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
        if (keyEventReader == null) {
            keyEventReader = new KeyEventItemReader<>(client, codec);
            keyEventReader.setDatabase(database);
            keyEventReader.setKeyPattern(keyPattern);
            keyEventReader.setFilter(e -> acceptKey(e.getKey()) && acceptKeyType(e.getType()));
            keyEventReader.setMeterRegistry(meterRegistry);
            keyEventReader.setName(getName() + "-key-event-reader");
            keyEventReader.setQueueCapacity(queueCapacity);
            keyEventReader.open(executionContext);
        }
        super.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        keyEventReader.update(executionContext);
        super.update(executionContext);
    }

    @Override
    public synchronized void close() throws ItemStreamException {
        super.close();
        if (keyEventReader != null) {
            keyEventReader.close();
            keyEventReader = null;
        }
    }

    @Override
    public synchronized KeyValue<K> poll(long timeout, TimeUnit unit) throws Exception {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        HashSet<KeyEvent<K>> keyEvents = new LinkedHashSet<>();
        KeyEvent<K> keyEvent;
        while (keyEvents.size() < batchSize && (keyEvent = keyEventReader.poll(timeout, unit)) != null) {
            keyEvents.add(keyEvent);
        }
        List<KeyValue<K>> keyValues = read(new ArrayList<>(keyEvents));
        iterator = keyValues.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    protected synchronized KeyValue<K> doRead() {
        throw new UnsupportedOperationException();
    }

    public static RedisLiveItemReader<byte[], byte[]> dump() {
        return new RedisLiveItemReader<>(ByteArrayCodec.INSTANCE, KeyValueRead.dump(ByteArrayCodec.INSTANCE));
    }

    public static RedisLiveItemReader<String, String> struct() {
        return struct(StringCodec.UTF8);
    }

    public static <K, V> RedisLiveItemReader<K, V> struct(RedisCodec<K, V> codec) {
        return new RedisLiveItemReader<>(codec, KeyValueRead.struct(codec));
    }

    public static RedisLiveItemReader<String, String> type() {
        return type(StringCodec.UTF8);
    }

    public static <K, V> RedisLiveItemReader<K, V> type(RedisCodec<K, V> codec) {
        return new RedisLiveItemReader<>(codec, KeyValueRead.type(codec));
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int capacity) {
        this.queueCapacity = capacity;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

}
