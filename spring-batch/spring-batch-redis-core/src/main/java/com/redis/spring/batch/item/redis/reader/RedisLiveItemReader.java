package com.redis.spring.batch.item.redis.reader;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.RedisOperation;

import io.lettuce.core.codec.RedisCodec;
import lombok.ToString;

@ToString
public class RedisLiveItemReader<K, V> extends RedisItemReader<K, V> implements PollableItemReader<KeyValue<K>> {

    public static final String COUNTER_DESCRIPTION = "Number of key events received. Status SUCCESS means key was successfully processed, 'FAILURE' means queue was full.";

    public static final int DEFAULT_QUEUE_CAPACITY = KeyEventItemReader.DEFAULT_QUEUE_CAPACITY;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private int database;

    private KeyEventItemReader<K, V> keyEventReader;

    private Iterator<KeyValue<K>> iterator = Collections.emptyIterator();;

    public RedisLiveItemReader(RedisCodec<K, V> codec, RedisOperation<K, V, KeyEvent<K>, KeyValue<K>> operation) {
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
        List<KeyValue<K>> keyValues = read(keyEvents);
        iterator = keyValues.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    protected synchronized KeyValue<K> doRead() throws Exception {
        throw new UnsupportedOperationException();
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
