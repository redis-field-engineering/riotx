package com.redis.spring.batch.item.redis.reader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.observability.BatchMetrics;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.data.util.Predicates;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.BatchRedisMetrics;
import com.redis.spring.batch.UniqueBlockingQueue;
import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

public class KeyEventItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<KeyEvent<K>>
        implements PollableItemReader<KeyEvent<K>> {

    public static final int DEFAULT_QUEUE_CAPACITY = 10000;

    private static final String METRIC_NAME = "key.event";

    public static final String COUNTER_DESCRIPTION = "Number of key events received. Status SUCCESS means key was successfully processed, 'FAILURE' means queue was full.";

    private final Log log = LogFactory.getLog(getClass());

    private final KeyEventListenerContainer<K, V> listenerContainer;

    private MeterRegistry meterRegistry = Metrics.globalRegistry;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private int database;

    private String keyPattern;

    private Predicate<KeyEvent<K>> filter = Predicates.isTrue();

    protected BlockingQueue<KeyEvent<K>> queue;

    public KeyEventItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        setName(ClassUtils.getShortName(getClass()));
        this.listenerContainer = KeyEventListenerContainer.create(client, codec);
    }

    @Override
    protected synchronized void doOpen() {
        if (queue == null) {
            log.info(String.format("Creating queue with capacity %,d", queueCapacity));
            queue = new UniqueBlockingQueue<>(queueCapacity);
            BatchRedisMetrics.gaugeQueue(meterRegistry, METRIC_NAME + ".queue", queue, nameTag());
        }
        if (!listenerContainer.isRunning()) {
            listenerContainer.receive(database, keyPattern, this::onKeyEvent);
            listenerContainer.start();
        }
    }

    private Tag nameTag() {
        return Tag.of("name", getName());
    }

    private void onKeyEvent(KeyEvent<K> keyEvent) {
        if (filter.test(keyEvent)) {
            boolean added = queue.offer(keyEvent);
            String status = added ? BatchMetrics.STATUS_SUCCESS : BatchMetrics.STATUS_FAILURE;
            Tags tags = BatchUtils.tags(keyEvent, status).and(nameTag());
            BatchRedisMetrics.createCounter(meterRegistry, METRIC_NAME, COUNTER_DESCRIPTION, tags).increment();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Key event key=%s event=%s type=%s: %s", BatchUtils.toString(keyEvent.getKey()),
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
    protected KeyEvent<K> doRead() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyEvent<K> poll(long timeout, TimeUnit unit) throws InterruptedException, Exception {
        return queue.poll(timeout, unit);
    }

    public BlockingQueue<KeyEvent<K>> getQueue() {
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

    public Predicate<KeyEvent<K>> getFilter() {
        return filter;
    }

    public void setFilter(Predicate<KeyEvent<K>> filter) {
        this.filter = filter;
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

}
