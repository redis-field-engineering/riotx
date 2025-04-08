package com.redis.spring.batch.item.redis.reader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.observability.BatchMetrics;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.BatchRedisMetrics;
import com.redis.spring.batch.UniqueBlockingQueue;
import com.redis.spring.batch.item.AbstractPollableItemReader;
import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

public class KeyEventItemReader<K, V> extends AbstractPollableItemReader<KeyEvent<K>> {

    public static final int DEFAULT_QUEUE_CAPACITY = 10000;

    private static final String METRIC_NAME = "key.event";

    public static final String COUNTER_DESCRIPTION = "Number of key events received. Status SUCCESS means key was successfully processed, 'FAILURE' means queue was full.";

    private final Log log = LogFactory.getLog(getClass());

    private MeterRegistry meterRegistry = Metrics.globalRegistry;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private int database;

    private String keyPattern;

    private String keyType;

    protected BlockingQueue<KeyEvent<K>> queue;

    private KeyEventListenerContainer<K, V> listenerContainer;

    public KeyEventItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        setName(ClassUtils.getShortName(getClass()));
        this.listenerContainer = KeyEventListenerContainer.create(client, codec);
    }

    private void onKeyEvent(KeyEvent<K> keyEvent) {
        if (keyType == null || keyType.equalsIgnoreCase(keyEvent.getType())) {
            boolean added = queue.offer(keyEvent);
            String status = added ? BatchMetrics.STATUS_SUCCESS : BatchMetrics.STATUS_FAILURE;
            Tags tags = BatchUtils.tags(keyEvent, status).and("name", getName());
            BatchRedisMetrics.createCounter(meterRegistry, METRIC_NAME, COUNTER_DESCRIPTION, tags).increment();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Key event key=%s event=%s type=%s: %s", BatchUtils.toString(keyEvent.getKey()),
                        keyEvent.getEvent(), keyEvent.getType(), status));
            }
        }
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
        if (!listenerContainer.isRunning()) {
            listenerContainer.receive(database, keyPattern, this::onKeyEvent);
            listenerContainer.start();
        }
    }

    @Override
    protected synchronized void doOpen() {
        if (queue == null) {
            Tag nameTag = Tag.of("name", getName());
            log.info(String.format("Creating queue with capacity %,d", queueCapacity));
            queue = new UniqueBlockingQueue<>(queueCapacity);
            BatchRedisMetrics.gaugeQueue(meterRegistry, METRIC_NAME + ".queue", queue, nameTag);
        }
    }

    public boolean isOpen() {
        return listenerContainer.isRunning();
    }

    @Override
    public synchronized void close() throws ItemStreamException {
        listenerContainer.stop();
        super.close();
    }

    @Override
    protected synchronized void doClose() {
        queue = null;
    }

    @Override
    public boolean isComplete() {
        return !listenerContainer.isRunning();
    }

    @Override
    protected KeyEvent<K> doPoll(long timeout, TimeUnit unit) throws Exception {
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

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String type) {
        this.keyType = type;
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

}
