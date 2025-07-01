package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.*;
import com.redis.batch.operation.KeyDumpRead;
import com.redis.batch.operation.KeyNoneRead;
import com.redis.batch.operation.KeyStructRead;
import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import lombok.ToString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@ToString
public class RedisLiveItemReader<K, V, T> extends RedisItemReader<K, V, T> implements PollableItemReader<T> {

    public static final int DEFAULT_QUEUE_CAPACITY = KeyEventItemReader.DEFAULT_QUEUE_CAPACITY;

    private final Log log = LogFactory.getLog(getClass());

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private int database;

    private KeyEventItemReader<K, V> keyEventReader;

    private Iterator<T> itemIterator = Collections.emptyIterator();

    protected MeterRegistry meterRegistry = Metrics.globalRegistry;

    private final AtomicLong counter = new AtomicLong();

    public RedisLiveItemReader(RedisCodec<K, V> codec, RedisBatchOperation<K, V, KeyEvent<K>, T> operation) {
        super(codec, operation);
    }

    public KeyEventItemReader<K, V> getKeyEventReader() {
        return keyEventReader;
    }

    @Override
    public synchronized void open(ExecutionContext context) throws ItemStreamException {
        if (keyEventReader == null) {
            keyEventReader = new KeyEventItemReader<>(client, codec);
            keyEventReader.setDatabase(database);
            keyEventReader.setKeyPattern(keyPattern);
            keyEventReader.setMeterRegistry(meterRegistry);
            keyEventReader.setName(getName() + "-key-event-reader");
            keyEventReader.setQueueCapacity(queueCapacity);
            keyEventReader.open(context);
        }
        super.open(context);
    }

    @Override
    public void update(ExecutionContext context) throws ItemStreamException {
        keyEventReader.update(context);
        super.update(context);
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
    public synchronized T poll(long timeout, TimeUnit unit) throws Exception {
        if (itemIterator.hasNext()) {
            try {
                return itemIterator.next();
            } finally {
                counter.incrementAndGet();
            }
        }
        Set<KeyEvent<K>> keyEvents = new LinkedHashSet<>();
        KeyEvent<K> keyEvent;
        while (keyEvents.size() < batchSize && (keyEvent = keyEventReader.poll(timeout, unit)) != null) {
            if (accept(keyEvent)) {
                keyEvents.add(keyEvent);
            }
        }
        List<T> items = read(new ArrayList<>(keyEvents));
        itemIterator = items.iterator();
        if (itemIterator.hasNext()) {
            try {
                return itemIterator.next();
            } finally {
                counter.incrementAndGet();
            }
        }
        return null;
    }

    private boolean accept(KeyEvent<K> keyEvent) {
        if (keyType == null) {
            return true;
        }
        return keyType.equalsIgnoreCase(KeyEvent.type(keyEvent.getEvent()).getName());
    }

    @Override
    protected synchronized T doRead() {
        throw new UnsupportedOperationException();
    }

    public static RedisLiveItemReader<byte[], byte[], KeyDumpEvent<byte[]>> dump() {
        return new RedisLiveItemReader<>(ByteArrayCodec.INSTANCE, new KeyDumpRead());
    }

    public static <K, V> RedisLiveItemReader<K, V, KeyStructEvent<K, V>> none(RedisCodec<K, V> codec) {
        return new RedisLiveItemReader<>(codec, new KeyNoneRead<>(codec));
    }

    public static <K, V> RedisLiveItemReader<K, V, KeyStructEvent<K, V>> struct(RedisCodec<K, V> codec) {
        return new RedisLiveItemReader<>(codec, new KeyStructRead<>(codec));
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

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

}
