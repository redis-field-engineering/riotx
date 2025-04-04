package com.redis.spring.batch.item.redis;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.BatchRedisMetrics;
import com.redis.spring.batch.UniqueBlockingQueue;
import com.redis.spring.batch.item.AbstractAsyncItemStreamSupport;
import com.redis.spring.batch.item.ChunkProcessingItemWriter;
import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.item.QueueItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.RedisOperation;
import com.redis.spring.batch.item.redis.common.OperationExecutor;
import com.redis.spring.batch.item.redis.reader.KeyEvent;
import com.redis.spring.batch.item.redis.reader.KeyEventItemReader;
import com.redis.spring.batch.item.redis.reader.KeyEventListener;
import com.redis.spring.batch.item.redis.reader.KeyScanEventItemReader;
import com.redis.spring.batch.item.redis.reader.KeyScanItemReader;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;
import com.redis.spring.batch.item.redis.reader.KeyValueStructRead;
import com.redis.spring.batch.item.redis.reader.RedisScanSizeEstimator;
import com.redis.spring.batch.step.FlushingChunkProvider;
import com.redis.spring.batch.step.FlushingStepBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemReader<K, V> extends AbstractAsyncItemStreamSupport<KeyEvent<K>, KeyEvent<K>>
		implements PollableItemReader<KeyValue<K>> {

	public enum ReaderMode {
		SCAN, LIVE, LIVEONLY
	}

	public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);
	public static final int DEFAULT_SCAN_COUNT = 100;
	public static final int DEFAULT_POOL_SIZE = OperationExecutor.DEFAULT_POOL_SIZE;
	public static final int DEFAULT_EVENT_QUEUE_CAPACITY = KeyEventItemReader.DEFAULT_QUEUE_CAPACITY;
	public static final ReaderMode DEFAULT_MODE = ReaderMode.SCAN;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;
	public static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;
	public static final String QUEUE_GAUGE_NAME = METRICS_PREFIX + "queue";

	private final RedisCodec<K, V> codec;
	private final RedisOperation<K, V, KeyEvent<K>, KeyValue<K>> operation;

	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;
	private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	private ReaderMode mode = DEFAULT_MODE;
	private int poolSize = DEFAULT_POOL_SIZE;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private int eventQueueCapacity = DEFAULT_EVENT_QUEUE_CAPACITY;
	private long scanCount = DEFAULT_SCAN_COUNT;
	private ReadFrom readFrom;
	private String keyPattern;
	private String keyType;
	private int database;
	private KeyEventListener<K> keyEventListener;

	private AbstractRedisClient client;
	private BlockingQueue<KeyValue<K>> queue;

	public RedisItemReader(RedisCodec<K, V> codec, RedisOperation<K, V, KeyEvent<K>, KeyValue<K>> operation) {
		setName(ClassUtils.getShortName(getClass()));
		this.codec = codec;
		this.operation = operation;
	}

	@Override
	protected SimpleStepBuilder<KeyEvent<K>, KeyEvent<K>> stepBuilder() {
		SimpleStepBuilder<KeyEvent<K>, KeyEvent<K>> step = super.stepBuilder();
		if (mode == ReaderMode.SCAN) {
			return step;
		}
		FlushingStepBuilder<KeyEvent<K>, KeyEvent<K>> flushingStep = new FlushingStepBuilder<>(step);
		flushingStep.flushInterval(flushInterval);
		flushingStep.idleTimeout(idleTimeout);
		return flushingStep;
	}

	@Override
	protected ItemReader<KeyEvent<K>> reader() {
		switch (mode) {
		case LIVEONLY:
			return keyEventReader();
		case LIVE:
			return keyScanEventReader();
		default:
			return keyScanReader();
		}
	}

	@Override
	protected boolean jobRunning() {
		return super.jobRunning() && readerOpen();
	}

	@SuppressWarnings("unchecked")
	private boolean readerOpen() {
		switch (mode) {
		case LIVE:
		case LIVEONLY:
			return getReader() != null && ((KeyEventItemReader<K, V>) getReader()).isOpen();
		default:
			return true;
		}
	}

	private KeyScanItemReader<K, V> keyScanReader() {
		KeyScanItemReader<K, V> reader = new KeyScanItemReader<>(client, codec);
		setKeyReaderName(reader);
		reader.setReadFrom(readFrom);
		reader.setScanArgs(scanArgs());
		reader.setMeterRegistry(meterRegistry);
		return reader;
	}

	private KeyEventItemReader<K, V> keyEventReader() {
		KeyEventItemReader<K, V> reader = new KeyEventItemReader<>(client, codec);
		configure(reader);
		return reader;
	}

	private KeyScanEventItemReader<K, V> keyScanEventReader() {
		KeyScanEventItemReader<K, V> reader = new KeyScanEventItemReader<>(client, codec, keyScanReader());
		configure(reader);
		return reader;
	}

	private void setKeyReaderName(ItemStreamSupport reader) {
		reader.setName(getName() + "-key-reader");
	}

	private void configure(KeyEventItemReader<K, V> reader) {
		setKeyReaderName(reader);
		reader.setMeterRegistry(meterRegistry);
		reader.setQueueCapacity(eventQueueCapacity);
		reader.setDatabase(database);
		reader.setKeyPattern(keyPattern);
		reader.setKeyType(keyType);
		reader.setPollTimeout(pollTimeout);
		if (keyEventListener != null) {
			reader.setKeyEventListener(keyEventListener);
		}
	}

	@Override
	public KeyValue<K> read() throws InterruptedException, Exception {
		KeyValue<K> item;
		do {
			item = poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && !isComplete());
		return item;
	}

	@Override
	public KeyValue<K> poll(long timeout, TimeUnit unit) throws InterruptedException, Exception {
		return queue.poll(timeout, unit);
	}

	@Override
	protected ItemWriter<KeyEvent<K>> writer() {
		log.info(String.format("Creating queue with capacity %,d", queueCapacity));
		queue = new UniqueBlockingQueue<>(queueCapacity);
		BatchRedisMetrics.gaugeQueue(meterRegistry, QUEUE_GAUGE_NAME, queue, nameTag());
		QueueItemWriter<KeyValue<K>> queueWriter = new QueueItemWriter<>(queue);
		OperationExecutor<K, V, KeyEvent<K>, KeyValue<K>> executor = operationExecutor();
		executor.setMeterRegistry(meterRegistry);
		executor.setName(getName() + "-operation");
		return new ChunkProcessingItemWriter<>(executor, queueWriter);
	}

	public OperationExecutor<K, V, KeyEvent<K>, KeyValue<K>> operationExecutor() {
		Assert.notNull(client, getName() + ": Redis client not set");
		OperationExecutor<K, V, KeyEvent<K>, KeyValue<K>> executor = new OperationExecutor<>(codec, operation);
		executor.setClient(client);
		executor.setPoolSize(poolSize);
		executor.setReadFrom(readFrom);
		return executor;
	}

	private KeyScanArgs scanArgs() {
		KeyScanArgs args = new KeyScanArgs();
		args.limit(scanCount);
		if (keyPattern != null) {
			args.match(keyPattern);
		}
		if (keyType != null) {
			args.type(keyType);
		}
		return args;
	}

	public RedisScanSizeEstimator scanSizeEstimator() {
		return RedisScanSizeEstimator.from(this);
	}

	public static RedisItemReader<byte[], byte[]> dump() {
		return new RedisItemReader<>(ByteArrayCodec.INSTANCE, KeyValueRead.dump(ByteArrayCodec.INSTANCE));
	}

	public static RedisItemReader<String, String> type() {
		return type(StringCodec.UTF8);
	}

	public static <K, V> RedisItemReader<K, V> type(RedisCodec<K, V> codec) {
		return new RedisItemReader<>(codec, KeyValueRead.type(codec));
	}

	public static RedisItemReader<String, String> struct() {
		return struct(StringCodec.UTF8);
	}

	public static <K, V> RedisItemReader<K, V> struct(RedisCodec<K, V> codec) {
		return new RedisItemReader<>(codec, new KeyValueStructRead<>(codec));
	}

	public void setKeyEventListener(KeyEventListener<K> listener) {
		this.keyEventListener = listener;
	}

	public RedisOperation<K, V, KeyEvent<K>, KeyValue<K>> getOperation() {
		return operation;
	}

	public BlockingQueue<KeyValue<K>> getQueue() {
		return queue;
	}

	public RedisCodec<K, V> getCodec() {
		return codec;
	}

	public AbstractRedisClient getClient() {
		return client;
	}

	public void setClient(AbstractRedisClient client) {
		this.client = client;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int size) {
		this.poolSize = size;
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

	public long getScanCount() {
		return scanCount;
	}

	public void setScanCount(long count) {
		this.scanCount = count;
	}

	public ReadFrom getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public int getEventQueueCapacity() {
		return eventQueueCapacity;
	}

	public void setEventQueueCapacity(int capacity) {
		this.eventQueueCapacity = capacity;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public ReaderMode getMode() {
		return mode;
	}

	public void setMode(ReaderMode mode) {
		this.mode = mode;
	}

	public Duration getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(Duration interval) {
		this.flushInterval = interval;
	}

	public Duration getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Duration timeout) {
		this.idleTimeout = timeout;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int capacity) {
		this.queueCapacity = capacity;
	}

	public Duration getPollTimeout() {
		return pollTimeout;
	}

	public void setPollTimeout(Duration timeout) {
		this.pollTimeout = timeout;
	}

}
