package com.redis.riotx;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.Assert;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.RiotStep;
import com.redis.spring.batch.item.redis.reader.StreamItemReader;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "stream-import", description = "Import data from a Redis stream.")
public class StreamImportCommand extends AbstractTargetRedisImportCommand {

	private static final String TASK_NAME = "Migrating";

	@Parameters(arity = "1..*", mapFallbackValue = "0", description = "Stream(s) and corresponding offsets to import, in the form key=offset. If not specified offset defaults to ${MAP-FALLBACK-VALUE}.", paramLabel = "STREAM")
	private Map<String, String> streams;

	@ArgGroup(exclusive = false)
	private StreamReaderArgs streamReaderArgs = new StreamReaderArgs();

	@Override
	protected Job job() {
		Assert.isTrue(hasOperations(), "No Redis command specified");
		StreamItemReader<String, String> reader = reader();
		RiotStep<StreamMessage<String, String>, Map<String, Object>> step = new RiotStep<>(reader, operationWriter());
		step.processor(streamMessageProcessor());
		step.live(true);
		step.setFlushInterval(reader.getBlock());
		step.idleTimeout(reader.getPollTimeout());
		step.taskName(TASK_NAME);
		return job(step);
	}

	private StreamItemReader<String, String> reader() {
		log.info("Creating stream reader with streams {} {}", streams, streamReaderArgs);
		StreamItemReader<String, String> reader = new StreamItemReader<>(sourceRedisContext.getClient(),
				StringCodec.UTF8, streamOffsets());
		log.info("Configuring stream reader with read-from {}", sourceRedisContext.getReadFrom());
		reader.setReadFrom(sourceRedisContext.getReadFrom());
		streamReaderArgs.configure(reader);
		return reader;
	}

	@SuppressWarnings("unchecked")
	private StreamOffset<String>[] streamOffsets() {
		Assert.notEmpty(streams, "No stream specified");
		return streams.entrySet().stream().map(e -> StreamOffset.from(e.getKey(), e.getValue()))
				.toArray(StreamOffset[]::new);
	}

	protected ItemProcessor<StreamMessage<String, String>, Map<String, Object>> streamMessageProcessor() {
		return RiotUtils.processor(new FunctionItemProcessor<>(this::messageBody), super.processor());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Map<String, Object> messageBody(StreamMessage<String, String> message) {
		return (Map) message.getBody();
	}

	public Map<String, String> getStreams() {
		return streams;
	}

	public void setStreams(Map<String, String> streams) {
		this.streams = streams;
	}

	public StreamReaderArgs getStreamReaderArgs() {
		return streamReaderArgs;
	}

	public void setStreamReaderArgs(StreamReaderArgs streamReaderArgs) {
		this.streamReaderArgs = streamReaderArgs;
	}

}
