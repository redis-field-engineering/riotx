package com.redis.riotx;

import java.time.Duration;

import com.redis.spring.batch.item.redis.reader.StreamItemReader;
import com.redis.spring.batch.item.redis.reader.StreamItemReader.AckPolicy;

import io.lettuce.core.Consumer;
import picocli.CommandLine.Option;

public class StreamReaderArgs {

	public static final String DEFAULT_OFFSET = StreamItemReader.DEFAULT_OFFSET;
	public static final String DEFAULT_CONSUMER_GROUP = "riotx-" + RiotxVersion.getVersion();
	public static final AckPolicy DEFAULT_ACK_POLICY = StreamItemReader.DEFAULT_ACK_POLICY;
	public static final Duration DEFAULT_BLOCK = StreamItemReader.DEFAULT_BLOCK;
	public static final long DEFAULT_COUNT = StreamItemReader.DEFAULT_COUNT;

	@Option(names = "--idle-timeout", description = "Min duration in seconds to consider reader complete (default: no timeout).", paramLabel = "<sec>")
	private long idleTimeout;

	@Option(names = "--ack", description = "Stream reader ack policy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<off>")
	private AckPolicy ackPolicy = DEFAULT_ACK_POLICY;

	@Option(names = "--block", description = "Stream xread block duration in millis (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long block = DEFAULT_BLOCK.toMillis();

	@Option(names = "--count", description = "Stream xread count (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long count = DEFAULT_COUNT;

	@Option(names = "--max", description = "Max number of messages to import.", paramLabel = "<count>")
	private int maxItemCount;

	@Option(names = "--consumer-group", description = "Stream consumer group (default: ${DEFAULT-VALUE}).", paramLabel = "<id>")
	private String consumerGroup = DEFAULT_CONSUMER_GROUP;

	@Option(names = "--consumer", description = "Stream consumer name. Enables stream processing in a consumer group.", paramLabel = "<name>")
	private String consumerName;

	public void configure(StreamItemReader<String, String> reader) {
		reader.setAckPolicy(ackPolicy);
		if (idleTimeout > 0) {
			reader.setPollTimeout(Duration.ofSeconds(idleTimeout));
		}
		reader.setBlock(Duration.ofMillis(block));
		reader.setCount(count);
		if (consumerName != null) {
			reader.setConsumer(Consumer.from(consumerGroup, consumerName));
		}
		if (maxItemCount > 0) {
			reader.setMaxItemCount(maxItemCount);
		}
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public AckPolicy getAckPolicy() {
		return ackPolicy;
	}

	public void setAckPolicy(AckPolicy ackPolicy) {
		this.ackPolicy = ackPolicy;
	}

	public long getBlock() {
		return block;
	}

	public void setBlock(long block) {
		this.block = block;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public int getMaxItemCount() {
		return maxItemCount;
	}

	public void setMaxItemCount(int maxItemCount) {
		this.maxItemCount = maxItemCount;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getConsumerName() {
		return consumerName;
	}

	public void setConsumerName(String consumerName) {
		this.consumerName = consumerName;
	}

	@Override
	public String toString() {
		return "StreamReaderArgs [ackPolicy=" + ackPolicy + ", block=" + block + ", count=" + count + ", maxItemCount="
				+ maxItemCount + ", consumerGroup=" + consumerGroup + ", consumerName=" + consumerName + "]";
	}

}
