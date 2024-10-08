package com.redis.riotx;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.Assert;

import com.redis.riot.AbstractImportCommand;
import com.redis.riot.RedisArgs;
import com.redis.riot.RedisContext;
import com.redis.riot.TargetRedisArgs;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.reader.StreamItemReader;

import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "stream-import", description = "Import data from a Redis stream.")
public class StreamImport extends AbstractImportCommand {

	private static final String TASK_NAME = "Importing";
	private static final String STEP_NAME = "step";

	@Parameters(arity = "1..*", mapFallbackValue = "0", description = "Stream(s) and corresponding offsets to import, in the form key=offset. If not specified offset defaults to ${MAP-FALLBACK-VALUE}.", paramLabel = "STREAM")
	private Map<String, String> streams;

	@ArgGroup(exclusive = false)
	private RedisArgs sourceRedisArgs = new RedisArgs();

	@Option(names = "--target-uri", description = "Target server URI or endpoint in the form host:port. Source endpoint is used if not specified.", paramLabel = "<uri>")
	private RedisURI targetRedisUri;

	@ArgGroup(exclusive = false)
	private TargetRedisArgs targetRedisArgs = new TargetRedisArgs();

	@ArgGroup(exclusive = false)
	private StreamReaderArgs streamReaderArgs = new StreamReaderArgs();

	protected RedisContext sourceRedisContext;

	@Override
	protected void execute() throws Exception {
		sourceRedisContext = sourceRedisContext();
		try {
			super.execute();
		} finally {
			sourceRedisContext.close();
		}
	}

	private RedisContext sourceRedisContext() {
		log.info("Creating source Redis context with {}", sourceRedisArgs);
		return sourceRedisArgs.redisContext();
	}

	@Override
	protected RedisContext targetRedisContext() {
		if (targetRedisUri == null) {
			log.info("No target URI specified, using source Redis context for target");
			return sourceRedisContext();
		}
		log.info("Creating target Redis context with {} {} {}", targetRedisUri, targetRedisArgs,
				sourceRedisArgs.getSslArgs());
		return RedisContext.create(targetRedisArgs.redisURI(targetRedisUri), targetRedisArgs.isCluster(),
				targetRedisArgs.getProtocolVersion(), sourceRedisArgs.getSslArgs());
	}

	@Override
	protected void configureTargetRedisWriter(RedisItemWriter<?, ?, ?> writer) {
		super.configureTargetRedisWriter(writer);
		writer.setPoolSize(targetRedisArgs.getPoolSize());
	}

	@Override
	protected Job job() throws Exception {
		Assert.isTrue(hasOperations(), "No Redis command specified");
		StreamItemReader<String, String> reader = reader();
		RedisItemWriter<String, String, Map<String, Object>> writer = operationWriter();
		configureTargetRedisWriter(writer);
		Step<StreamMessage<String, String>, Map<String, Object>> step = new Step<>(STEP_NAME, reader, writer);
		step.processor(streamMessageProcessor());
		step.live(true);
		step.flushInterval(reader.getBlock());
		step.idleTimeout(reader.getPollTimeout());
		step.taskName(TASK_NAME);
		return job(step);
	}

	private StreamItemReader<String, String> reader() {
		log.info("Creating stream reader with streams {} and {}", streams, streamReaderArgs);
		StreamItemReader<String, String> reader = new StreamItemReader<>(sourceRedisContext.getClient(),
				StringCodec.UTF8, streamOffsets());
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

	public RedisArgs getSourceRedisArgs() {
		return sourceRedisArgs;
	}

	public void setSourceRedisArgs(RedisArgs sourceRedisArgs) {
		this.sourceRedisArgs = sourceRedisArgs;
	}

	public RedisURI getTargetRedisUri() {
		return targetRedisUri;
	}

	public void setTargetRedisUri(RedisURI targetRedisUri) {
		this.targetRedisUri = targetRedisUri;
	}

	public TargetRedisArgs getTargetRedisArgs() {
		return targetRedisArgs;
	}

	public void setTargetRedisArgs(TargetRedisArgs targetRedisArgs) {
		this.targetRedisArgs = targetRedisArgs;
	}

	public StreamReaderArgs getStreamReaderArgs() {
		return streamReaderArgs;
	}

	public void setStreamReaderArgs(StreamReaderArgs streamReaderArgs) {
		this.streamReaderArgs = streamReaderArgs;
	}

}
