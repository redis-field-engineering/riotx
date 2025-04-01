package com.redis.spring.batch.memcached;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.item.AbstractAsyncItemStreamSupport;
import com.redis.spring.batch.item.ChunkProcessingItemWriter;
import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.item.QueueItemWriter;
import com.redis.spring.batch.memcached.reader.LruMetadumpEntry;
import com.redis.spring.batch.memcached.reader.LruMetadumpItemProcessor;
import com.redis.spring.batch.memcached.reader.LruMetadumpItemReader;

import net.spy.memcached.MemcachedClient;

public class MemcachedItemReader extends AbstractAsyncItemStreamSupport<LruMetadumpEntry, LruMetadumpEntry>
		implements PollableItemReader<MemcachedEntry> {

	public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;

	private final Supplier<MemcachedClient> clientSupplier;

	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	private BlockingQueue<MemcachedEntry> queue;

	public MemcachedItemReader(Supplier<MemcachedClient> clientSupplier) {
		setName(ClassUtils.getShortName(getClass()));
		this.clientSupplier = clientSupplier;
	}

	@Override
	protected ItemReader<LruMetadumpEntry> reader() {
		return new LruMetadumpItemReader(clientSupplier.get());
	}

	@Override
	protected ItemWriter<LruMetadumpEntry> writer() {
		queue = new LinkedBlockingQueue<>(queueCapacity);
		LruMetadumpItemProcessor processor = new LruMetadumpItemProcessor(clientSupplier.get());
		return new ChunkProcessingItemWriter<>(processor, new QueueItemWriter<>(queue));
	}

	@Override
	public MemcachedEntry read() throws InterruptedException, Exception {
		MemcachedEntry item;
		do {
			item = poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && !isComplete());
		return item;
	}

	@Override
	public MemcachedEntry poll(long timeout, TimeUnit unit) throws InterruptedException, Exception {
		return queue.poll(timeout, unit);
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	public Duration getPollTimeout() {
		return pollTimeout;
	}

	public void setPollTimeout(Duration timeout) {
		this.pollTimeout = timeout;
	}

}
