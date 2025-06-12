package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyValue;
import com.redis.spring.batch.UniqueBlockingQueue;
import com.redis.spring.batch.item.AbstractCountingItemReader;
import com.redis.spring.batch.item.PollableItemReader;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.observability.BatchMetrics;
import org.springframework.data.util.Predicates;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class KeyEventItemReader<K, V> extends AbstractCountingItemReader<KeyValue<K>>
        implements PollableItemReader<KeyValue<K>> {

    public static final int DEFAULT_QUEUE_CAPACITY = 10000;

    private static final String METRIC_NAME = "key.event";

    public static final String COUNTER_DESCRIPTION = "Number of key events received. Status SUCCESS means key was successfully processed, 'FAILURE' means queue was full.";

    private final Log log = LogFactory.getLog(getClass());

    private final KeyEventListenerContainer<K, V> listenerContainer;

    private MeterRegistry meterRegistry = Metrics.globalRegistry;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private int database;

    private String keyPattern;

    private Predicate<KeyValue<K>> filter = Predicates.isTrue();

    protected BlockingQueue<KeyValue<K>> queue;

    public KeyEventItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        this.listenerContainer = KeyEventListenerContainer.create(client, codec);
    }

    @Override
    protected synchronized void doOpen() {
        if (queue == null) {
            log.info(String.format("Creating queue with capacity %,d", queueCapacity));
            queue = new UniqueBlockingQueue<>(queueCapacity);
            BatchUtils.gaugeQueue(meterRegistry, METRIC_NAME + ".queue", queue, nameTag());
        }
        if (!listenerContainer.isRunning()) {
            listenerContainer.receive(database, keyPattern, this::onKeyEvent);
            listenerContainer.start();
        }
    }

    private Tag nameTag() {
        return Tag.of("name", getName());
    }

    private void onKeyEvent(KeyValue<K> keyEvent) {
        if (filter.test(keyEvent)) {
            boolean added = queue.offer(keyEvent);
            String status = added ? BatchMetrics.STATUS_SUCCESS : BatchMetrics.STATUS_FAILURE;
            Tags tags = BatchUtils.tags(keyEvent, status).and(nameTag());
            BatchUtils.createCounter(meterRegistry, METRIC_NAME, COUNTER_DESCRIPTION, tags).increment();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Key event key=%s event=%s type=%s: %s", BatchUtils.keyToString(keyEvent.getKey()),
                        keyEvent.getEvent(), keyEvent.getType(), status));
            }
        }
    }

    public boolean isOpen() {
        return listenerContainer.isRunning();
    }

    @Override
    protected synchronized void doClose() {
        if (listenerContainer.isRunning()) {
            listenerContainer.stop();
        }
        queue = null;
    }

    @Override
    protected KeyValue<K> doRead() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValue<K> poll(long timeout, TimeUnit unit) throws Exception {
        return queue.poll(timeout, unit);
    }

    public BlockingQueue<KeyValue<K>> getQueue() {
        return queue;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getKeyPattern() {
        return keyPattern;
    }

    public void setKeyPattern(String pattern) {
        this.keyPattern = pattern;
    }

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public Predicate<KeyValue<K>> getFilter() {
        return filter;
    }

    public void setFilter(Predicate<KeyValue<K>> filter) {
        this.filter = filter;
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

}
