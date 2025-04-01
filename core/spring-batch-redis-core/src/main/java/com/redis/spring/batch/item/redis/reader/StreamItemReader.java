package com.redis.spring.batch.item.redis.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.item.redis.common.Key;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.codec.RedisCodec;

public class StreamItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<StreamMessage<K, V>>
		implements PollableItemReader<StreamMessage<K, V>> {

	public enum AckPolicy {
		AUTO, MANUAL
	}

	public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(Long.MAX_VALUE);
	public static final String DEFAULT_OFFSET = "0-0";
	public static final Duration DEFAULT_BLOCK = Duration.ofMillis(100);
	public static final long DEFAULT_COUNT = 50;
	public static final AckPolicy DEFAULT_ACK_POLICY = AckPolicy.AUTO;

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final Map<Key<K>, String> offsets;

	private Consumer<K> consumer;
	private Duration block = DEFAULT_BLOCK;
	private long count = DEFAULT_COUNT;
	private AckPolicy ackPolicy = DEFAULT_ACK_POLICY;
	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

	private StatefulRedisModulesConnection<K, V> connection;
	private Iterator<StreamMessage<K, V>> iterator = Collections.emptyIterator();
	private MessageReader<K, V> reader;
	private RedisStreamCommands<K, V> commands;
	private ReadFrom readFrom;

	@SuppressWarnings("unchecked")
	public StreamItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, K... keys) {
		this(client, codec,
				Stream.of(keys).map(k -> StreamOffset.from(k, DEFAULT_OFFSET)).toArray(StreamOffset[]::new));
	}

	@SuppressWarnings("unchecked")
	public StreamItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, StreamOffset<K>... offsets) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
		this.offsets = Stream.of(offsets).collect(Collectors.toMap(this::key, StreamOffset::getOffset));
	}

	private Key<K> key(StreamOffset<K> offset) {
		return new Key<>(offset.getName());
	}

	private XReadArgs args(long blockMillis) {
		return XReadArgs.Builder.count(count).block(blockMillis);
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		if (reader == null) {
			connection = RedisModulesUtils.connection(client, codec, readFrom);
			commands = connection.sync();
			if (consumer != null) {
				offsets.forEach(this::createConsumerGroup);
			}
			reader = reader();
		}
	}

	private void createConsumerGroup(Key<K> key, String offset) {
		XGroupCreateArgs args = XGroupCreateArgs.Builder.mkstream(true);
		try {
			commands.xgroupCreate(StreamOffset.from(key.getKey(), offset), consumer.getGroup(), args);
		} catch (RedisBusyException e) {
			// Consumer Group name already exists, ignore
		}
	}

	@Override
	protected synchronized void doClose() throws Exception {
		reader = null;
		if (connection != null) {
			connection.close();
			connection = null;
		}
		commands = null;
	}

	private MessageReader<K, V> reader() {
		if (consumer == null) {
			return new SimpleMessageReader();
		}
		if (ackPolicy == AckPolicy.MANUAL) {
			return new ExplicitAckPendingMessageReader();
		}
		return new AutoAckPendingMessageReader();
	}

	@Override
	protected StreamMessage<K, V> doRead() throws Exception {
		return poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public synchronized StreamMessage<K, V> poll(long timeout, TimeUnit unit) {
		if (!iterator.hasNext()) {
			long remaining = unit.toMillis(timeout);
			List<StreamMessage<K, V>> messages;
			do {
				long start = System.currentTimeMillis();
				messages = reader.read(block.toMillis());
				remaining -= (System.currentTimeMillis() - start);
			} while (remaining > 0 && messages.isEmpty());
			if (CollectionUtils.isEmpty(messages)) {
				return null;
			}
			iterator = messages.iterator();
		}
		return iterator.next();

	}

	public List<StreamMessage<K, V>> readMessages() {
		return reader.read(block.toMillis());
	}

	public void ack(Iterable<? extends StreamMessage<K, V>> messages) {
		group(messages).values().stream().forEach(this::ack);
	}

	private void ack(List<StreamMessage<K, V>> messages) {
		group(messages).forEach(this::ack);
	}

	private void ack(Key<K> key, List<StreamMessage<K, V>> messages) {
		if (!CollectionUtils.isEmpty(messages)) {
			ack(key.getKey(), messages.stream().map(StreamMessage::getId).toArray(String[]::new));
		}

	}

	public long ack(K key, String... ids) {
		return commands.xack(key, consumer.getGroup(), ids);
	}

	private Map<Key<K>, List<StreamMessage<K, V>>> group(Iterable<? extends StreamMessage<K, V>> messages) {
		return StreamSupport.stream(messages.spliterator(), false).collect(Collectors.groupingBy(this::key));
	}

	private Key<K> key(StreamMessage<K, V> message) {
		return new Key<>(message.getStream());
	}

	private interface MessageReader<K, V> {

		/**
		 * Reads messages from a stream
		 * 
		 * @param redisCommands Synchronous executed commands for Streams
		 * @param args          Stream read command args
		 * @return list of messages retrieved from the stream or empty list if no
		 *         messages available
		 * @throws MessageReadException
		 */
		List<StreamMessage<K, V>> read(long blockMillis);

	}

	private class SimpleMessageReader implements MessageReader<K, V> {

		@Override
		public List<StreamMessage<K, V>> read(long blockMillis) {
			List<StreamMessage<K, V>> messages = commands.xread(args(blockMillis), offsets());
			group(messages).forEach(this::updateOffset);
			return messages;
		}

		private void updateOffset(Key<K> key, List<StreamMessage<K, V>> messages) {
			StreamMessage<K, V> lastMessage = messages.get(messages.size() - 1);
			offsets.put(key, lastMessage.getId());
		}

	}

	@SuppressWarnings("unchecked")
	private StreamOffset<K>[] offsets() {
		return offsets.entrySet().stream().map(e -> StreamOffset.from(e.getKey().getKey(), e.getValue()))
				.toArray(StreamOffset[]::new);
	}

	private class ExplicitAckPendingMessageReader implements MessageReader<K, V> {

		protected List<StreamMessage<K, V>> readMessages(XReadArgs args) {
			return recover(commands.xreadgroup(consumer, args, offsets()));
		}

		protected List<StreamMessage<K, V>> recover(List<StreamMessage<K, V>> allMessages) {
			List<StreamMessage<K, V>> recoveredMessages = new ArrayList<>();
			List<StreamMessage<K, V>> messagesToAck = new ArrayList<>();
			group(allMessages).forEach((key, messages) -> {
				StreamId recoveryId = StreamId.parse(offsets.get(key));
				for (StreamMessage<K, V> message : messages) {
					StreamId messageId = StreamId.parse(message.getId());
					if (messageId.compareTo(recoveryId) > 0) {
						recoveredMessages.add(message);
						offsets.put(key, message.getId());
					} else {
						messagesToAck.add(message);
					}
				}
			});
			ack(messagesToAck);
			return recoveredMessages;
		}

		protected MessageReader<K, V> messageReader() {
			return new ExplicitAckMessageReader();
		}

		@Override
		public List<StreamMessage<K, V>> read(long blockMillis) {
			List<StreamMessage<K, V>> messages;
			messages = readMessages(args(blockMillis));
			if (messages.isEmpty()) {
				reader = messageReader();
				return reader.read(blockMillis);
			}
			return messages;
		}

	}

	private class ExplicitAckMessageReader implements MessageReader<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		public List<StreamMessage<K, V>> read(long blockMillis) {
			return commands.xreadgroup(consumer, args(blockMillis), offsets.keySet().stream().map(Key::getKey)
					.map(StreamOffset::lastConsumed).toArray(StreamOffset[]::new));
		}

	}

	private class AutoAckPendingMessageReader extends ExplicitAckPendingMessageReader {

		@Override
		protected StreamItemReader.MessageReader<K, V> messageReader() {
			return new AutoAckMessageReader();
		}

		@Override
		protected List<StreamMessage<K, V>> recover(List<StreamMessage<K, V>> messages) {
			ack(messages);
			return Collections.emptyList();
		}

	}

	private class AutoAckMessageReader extends ExplicitAckMessageReader {

		@Override
		public List<StreamMessage<K, V>> read(long blockMillis) {
			List<StreamMessage<K, V>> messages = super.read(blockMillis);
			ack(messages);
			return messages;
		}

	}

	public ReadFrom getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public Duration getBlock() {
		return block;
	}

	public void setBlock(Duration block) {
		this.block = block;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public AckPolicy getAckPolicy() {
		return ackPolicy;
	}

	public void setAckPolicy(AckPolicy policy) {
		this.ackPolicy = policy;
	}

	public Consumer<K> getConsumer() {
		return consumer;
	}

	public void setConsumer(Consumer<K> consumer) {
		this.consumer = consumer;
	}

	public Duration getPollTimeout() {
		return pollTimeout;
	}

	public void setPollTimeout(Duration timeout) {
		this.pollTimeout = timeout;
	}

	public void setOffset(K key, String offset) {
		this.offsets.put(new Key<>(key), offset);
	}

	public void setOffset(StreamOffset<K> offset) {
		this.offsets.put(new Key<>(offset.getName()), offset.getOffset());
	}

}
