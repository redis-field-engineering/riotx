package com.redis.spring.batch.memcached;

import com.redis.spring.batch.item.AbstractCountingItemReader;
import com.redis.spring.batch.memcached.reader.LruCrawlerMetadumpOperation.Callback;
import com.redis.spring.batch.memcached.reader.LruCrawlerMetadumpOperationImpl;
import com.redis.spring.batch.memcached.reader.LruMetadumpEntry;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.transcoders.Transcoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

public class MemcachedItemReader extends AbstractCountingItemReader<MemcachedEntry> {

    public static final int DEFAULT_QUEUE_CAPACITY = 10000;

    public static final int DEFAULT_BATCH_SIZE = 50;

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(10);

    private final Transcoder<byte[]> transcoder = new ByteArrayTranscoder();

    private final Supplier<MemcachedClient> clientSupplier;

    private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

    private int batchSize = DEFAULT_BATCH_SIZE;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private MemcachedClient crawlerClient;

    private MemcachedClient readerClient;

    private BlockingQueue<LruMetadumpEntry> metadumpQueue;

    private CountDownLatch latch;

    private Iterator<MemcachedEntry> iterator = Collections.emptyIterator();

    public MemcachedItemReader(Supplier<MemcachedClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
    }

    @Override
    protected synchronized void doOpen() throws Exception {
        if (crawlerClient == null) {
            crawlerClient = clientSupplier.get();
        }
        if (readerClient == null) {
            readerClient = clientSupplier.get();
        }
        if (metadumpQueue == null) {
            metadumpQueue = new LinkedBlockingQueue<>(queueCapacity);
        }
        if (latch == null) {
            latch = crawlerClient.broadcastOp((n, l) -> new LruCrawlerMetadumpOperationImpl("all", new MetadumpCallback(l)));
        }
    }

    @Override
    protected synchronized void doClose() throws Exception {
        if (readerClient != null) {
            readerClient.shutdown();
            readerClient = null;
        }
        if (crawlerClient != null) {
            crawlerClient.shutdown();
            crawlerClient = null;
        }
    }

    @Override
    protected synchronized MemcachedEntry doRead() throws Exception {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        List<LruMetadumpEntry> entries = new ArrayList<>();
        do {
            LruMetadumpEntry entry = metadumpQueue.poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
            if (entry != null) {
                entries.add(entry);
            }
        } while (latch.getCount() != 0 && entries.size() < batchSize);
        iterator = read(entries).iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    public List<MemcachedEntry> read(Iterable<? extends LruMetadumpEntry> items) {
        Iterator<String> keys = StreamSupport.stream(items.spliterator(), false).map(LruMetadumpEntry::getKey).iterator();
        Map<String, byte[]> values = readerClient.getBulk(keys, transcoder);
        List<MemcachedEntry> entries = new ArrayList<>();
        for (LruMetadumpEntry metaEntry : items) {
            MemcachedEntry entry = new MemcachedEntry();
            entry.setKey(metaEntry.getKey());
            entry.setValue(values.get(metaEntry.getKey()));
            entry.setExpiration(expiration(metaEntry.getExp()));
            entries.add(entry);
        }
        return entries;
    }

    private Instant expiration(int expiration) {
        if (expiration > 0) {
            return Instant.ofEpochSecond(expiration);
        }
        return null;
    }

    private class MetadumpCallback implements Callback {

        private final Log log = LogFactory.getLog(getClass());

        private final CountDownLatch latch;

        public MetadumpCallback(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void gotMetadump(LruMetadumpEntry entry) {
            try {
                metadumpQueue.put(entry);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        @Override
        public void receivedStatus(OperationStatus status) {
            if (!status.isSuccess()) {
                log.error("Unsuccessful lru_crawler metadump: " + status);
            }
        }

        @Override
        public void complete() {
            latch.countDown();
        }

    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int capacity) {
        this.queueCapacity = capacity;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int size) {
        this.batchSize = size;
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

}
