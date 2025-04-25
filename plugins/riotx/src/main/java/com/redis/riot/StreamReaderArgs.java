package com.redis.riot;

import java.time.temporal.ChronoUnit;

import com.redis.riot.core.RiotDuration;
import com.redis.spring.batch.item.redis.reader.StreamItemReader;
import com.redis.spring.batch.item.redis.reader.StreamItemReader.AckPolicy;

import io.lettuce.core.Consumer;
import picocli.CommandLine.Option;

public class StreamReaderArgs {

	public static final String DEFAULT_OFFSET = StreamItemReader.DEFAULT_OFFSET;
	public static final String DEFAULT_CONSUMER_GROUP = "riotx-" + RiotVersion.getVersion();
	public static final AckPolicy DEFAULT_ACK_POLICY = StreamItemReader.DEFAULT_ACK_POLICY;
	public static final RiotDuration DEFAULT_BLOCK = RiotDuration.of(StreamItemReader.DEFAULT_BLOCK, ChronoUnit.MILLIS);
	public static final long DEFAULT_COUNT = StreamItemReader.DEFAULT_COUNT;

	@Option(names = "--idle-timeout", description = "Min duration to consider reader complete (default: no timeout).", paramLabel = "<dur>")
	private RiotDuration idleTimeout;

	@Option(names = "--ack", description = "Stream reader ack policy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<off>")
	private AckPolicy ackPolicy = DEFAULT_ACK_POLICY;

	@Option(names = "--block", description = "Stream xread block duration (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
	private RiotDuration block = DEFAULT_BLOCK;

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
		if (idleTimeout != null) {
			reader.setPollTimeout(idleTimeout.getValue());
		}
		reader.setBlock(block.getValue());
		reader.setCount(count);
		if (consumerName != null) {
			reader.setConsumer(Consumer.from(consumerGroup, consumerName));
		}
		if (maxItemCount > 0) {
			reader.setMaxItemCount(maxItemCount);
		}
	}

	public RiotDuration getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(RiotDuration idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public AckPolicy getAckPolicy() {
		return ackPolicy;
	}

	public void setAckPolicy(AckPolicy ackPolicy) {
		this.ackPolicy = ackPolicy;
	}

	public RiotDuration getBlock() {
		return block;
	}

	public void setBlock(RiotDuration block) {
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
