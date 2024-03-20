package com.redis.spring.batch.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import com.redis.spring.batch.memcached.impl.ByteArrayTranscoder;
import com.redis.spring.batch.memcached.impl.LruCrawlerMetadumpOperation;
import com.redis.spring.batch.memcached.impl.LruCrawlerMetadumpOperationImpl;
import com.redis.spring.batch.memcached.impl.LruMetadumpEntry;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.transcoders.Transcoder;

public class MemcachedItemReader implements ItemStreamReader<MemcachedEntry> {

	public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final int DEFAULT_CHUNK_SIZE = 50;

	private final Log log = LogFactory.getLog(MemcachedItemReader.class);

	private final List<InetSocketAddress> addresses;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int threads = DEFAULT_THREADS;
	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private BlockingQueue<MemcachedEntry> queue;
	private BlockingQueue<LruMetadumpEntry> metadumpEntryQueue;

	private MemcachedClient crawlerClient;
	private MemcachedClient processorClient;
	private CountDownLatch latch;
	private ExecutorService executor;
	private List<Future<Long>> futures;

	public MemcachedItemReader(InetSocketAddress... addresses) {
		this(Arrays.asList(addresses));
	}

	public MemcachedItemReader(List<InetSocketAddress> addresses) {
		this.addresses = addresses;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public void setPollTimeout(Duration pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		if (crawlerClient == null) {
			try {
				crawlerClient = client();
			} catch (IOException e) {
				throw new ItemStreamException("Could not initialize crawler client", e);
			}
		}
		if (processorClient == null) {
			try {
				processorClient = client();
			} catch (IOException e) {
				throw new ItemStreamException("Could not initialize processor client", e);
			}
		}
		if (queue == null) {
			queue = new LinkedBlockingQueue<>(queueCapacity);
		}
		if (metadumpEntryQueue == null) {
			metadumpEntryQueue = new LinkedBlockingQueue<>(queueCapacity);
		}
		if (latch == null) {
			latch = crawlerClient
					.broadcastOp((n, l) -> new LruCrawlerMetadumpOperationImpl("all", new MetadumpCallback(l)));
		}
		if (executor == null) {
			executor = Executors.newFixedThreadPool(threads);
		}
		if (futures == null) {
			futures = IntStream.range(0, threads).mapToObj(Processor::new).map(executor::submit)
					.collect(Collectors.toList());
		}
	}

	private MemcachedClient client() throws IOException {
		return new MemcachedClient(addresses);
	}

	@Override
	public synchronized void close() {
		if (executor != null) {
			executor.shutdownNow();
		}
		futures = null;
		latch = null;
		metadumpEntryQueue = null;
		queue = null;
		if (processorClient != null) {
			processorClient.shutdown();
			processorClient = null;
		}
		if (crawlerClient != null) {
			crawlerClient.shutdown();
			crawlerClient = null;
		}
	}

	@Override
	public MemcachedEntry read()
			throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		MemcachedEntry entry;
		do {
			entry = queue.poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} while (entry == null && !isDone());
		return entry;
	}

	private boolean isDone() {
		return futures == null || futures.stream().allMatch(Future::isDone);
	}

	private class Processor implements Callable<Long> {

		private static final Transcoder<byte[]> transcoder = new ByteArrayTranscoder();
		@SuppressWarnings("unused")
		private final int id;

		public Processor(int id) {
			this.id = id;
		}

		@Override
		public Long call() throws Exception {
			long added = 0;
			while (latch.getCount() > 0 || !metadumpEntryQueue.isEmpty()) {
				List<LruMetadumpEntry> metaEntries = new ArrayList<>(chunkSize);
				metadumpEntryQueue.drainTo(metaEntries, chunkSize);
				List<String> keys = metaEntries.stream().map(LruMetadumpEntry::getKey).collect(Collectors.toList());
				Map<String, byte[]> values = processorClient.getBulk(keys, transcoder);
				for (LruMetadumpEntry metaEntry : metaEntries) {
					MemcachedEntry entry = new MemcachedEntry();
					entry.setKey(metaEntry.getKey());
					entry.setValue(values.get(metaEntry.getKey()));
					entry.setExpiration(metaEntry.getExp());
					queue.put(entry);
					added++;
				}
			}
			return added;
		}

	}

	private class MetadumpCallback implements LruCrawlerMetadumpOperation.Callback {

		private final CountDownLatch latch;

		public MetadumpCallback(CountDownLatch latch) {
			this.latch = latch;
		}

		@Override
		public void gotMetadump(LruMetadumpEntry entry) {
			try {
				metadumpEntryQueue.put(entry);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException("Could not add entry to queue", e);
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

}
