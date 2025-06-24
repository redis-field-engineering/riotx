package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyEvent;
import com.redis.spring.batch.item.AbstractCountingPollableItemReader;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KeyEventItemReader<K, V> extends AbstractCountingPollableItemReader<KeyEvent<K>> {

    public static final int DEFAULT_QUEUE_CAPACITY = 10000;

    private static final String METRIC_NAME = "key.event";

    public static final String COUNTER_DESCRIPTION = "Number of key events received. Status SUCCESS means key was successfully processed, 'FAILURE' means queue was full.";

    private final Log log = LogFactory.getLog(getClass());

    private final KeyEventListenerContainer<K, V> listenerContainer;

    private MeterRegistry meterRegistry = Metrics.globalRegistry;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private int database;

    private String keyPattern;

    protected BlockingQueue<KeyEvent<K>> queue;

    public KeyEventItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        this.listenerContainer = KeyEventListenerContainer.create(client, codec);
    }

    @Override
    protected synchronized void doOpen() {
        if (queue == null) {
            log.info(String.format("Creating queue with capacity %,d", queueCapacity));
            queue = new LinkedBlockingQueue<>(queueCapacity);
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

    private void onKeyEvent(KeyEvent<K> keyEvent) {
        boolean added = offer(keyEvent);
        if (log.isDebugEnabled()) {
            String stringKey = BatchUtils.keyToString(keyEvent.getKey());
            log.debug(String.format("%s key=%s event=%s", added ? "Added" : "Could not add", stringKey, keyEvent.getEvent()));
        }
        Tags tags = BatchUtils.tags(keyEvent.getEvent(), keyEvent.getType(), added);
        BatchUtils.createCounter(meterRegistry, METRIC_NAME, COUNTER_DESCRIPTION, tags).increment();
    }

    private synchronized boolean offer(KeyEvent<K> event) {
        // Find existing event with same key (if any)
        for (KeyEvent<K> existing : queue) {
            if (BatchUtils.keyEquals(existing.getKey(), event.getKey())) {
                // There can only be one existing event with same key in the queue
                if (event.getTimestamp().isAfter(existing.getTimestamp())) {
                    // Remove older event and add newer one
                    queue.remove(existing);
                    return queue.offer(event);
                }
                // New event is older or same age, ignore it
                return true;
            }
        }
        // No existing event with this key, just add it
        return queue.offer(event);
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

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

}
