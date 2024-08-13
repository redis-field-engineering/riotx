package com.redis.riotx;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.core.Job;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.riot.AbstractExportCommand;
import com.redis.riot.AbstractTargetCommand;
import com.redis.riot.RedisWriterArgs;
import com.redis.riot.TargetRedisExecutionContext;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyEventStatus;
import com.redis.spring.batch.item.redis.reader.KeyNotificationItemReader;
import com.redis.spring.batch.item.redis.reader.RedisScanSizeEstimator;
import com.redis.spring.batch.item.redis.writer.impl.Xadd;

import io.lettuce.core.StreamMessage;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "stream-export", description = "Export Redis data to a Redis stream.")
public class StreamExport extends AbstractTargetCommand {

	public static final String STEP_NAME = "stream-export";
	public static final String DEFAULT_STREAM = "stream:export";

	private static final String QUEUE_MESSAGE = " | capacity: %,d | dropped: %,d";
	private static final String TASK_NAME = "Streaming";
	private static final ObjectMapper jsonMapper = jsonMapper();

	@ArgGroup(exclusive = false)
	private RedisWriterArgs targetRedisWriterArgs = new RedisWriterArgs();

	@Option(names = "--stream", description = "Target stream key (default: ${DEFAULT-VALUE}).", paramLabel = "<key>")
	private String stream = DEFAULT_STREAM;

	private static ObjectMapper jsonMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.setSerializationInclusion(Include.NON_DEFAULT);
		return mapper;
	}

	@Override
	protected Job job(TargetRedisExecutionContext context) {
		return job(context, step(context));
	}

	protected void configureTargetWriter(TargetRedisExecutionContext context, RedisItemWriter<?, ?, ?> writer) {
		log.info("Configuring target Redis writer with {}", targetRedisWriterArgs);
		targetRedisWriterArgs.configure(writer);
		context.configureTargetWriter(writer);
	}

	private Step<KeyValue<String, Object>, StreamMessage<String, String>> step(TargetRedisExecutionContext context) {
		RedisItemReader<String, String, Object> reader = RedisItemReader.struct();
		configureSourceReader(context, reader);
		RedisItemWriter<String, String, StreamMessage<String, String>> writer = writer();
		configureTargetWriter(context, writer);
		Step<KeyValue<String, Object>, StreamMessage<String, String>> step = new Step<>(STEP_NAME, reader, writer);
		step.processor(this::process);
		step.taskName(TASK_NAME);
		AbstractExportCommand.configure(step);
		if (reader.getMode() != ReaderMode.SCAN) {
			step.statusMessageSupplier(() -> liveExtraMessage(reader));
		}
		step.maxItemCountSupplier(RedisScanSizeEstimator.from(reader));
		return step;
	}

	private RedisItemWriter<String, String, StreamMessage<String, String>> writer() {
		return RedisItemWriter.operation(new Xadd<>(t -> stream, Arrays::asList));
	}

	private StreamMessage<String, String> process(KeyValue<String, Object> struct) throws JsonProcessingException {
		Map<String, String> body = new LinkedHashMap<>();
		body.put("key", struct.getKey());
		body.put("time", String.valueOf(struct.getTimestamp()));
		body.put("type", struct.getType());
		body.put("ttl", String.valueOf(struct.getTtl()));
		body.put("mem", String.valueOf(struct.getMemoryUsage()));
		body.put("value", value(struct));
		return new StreamMessage<>(stream, null, body);
	}

	private String value(KeyValue<String, Object> struct) throws JsonProcessingException {
		DataType type = KeyValue.type(struct);
		if (type == null) {
			return null;
		}
		switch (type) {
		case STRING:
		case JSON:
			return (String) struct.getValue();
		default:
			return jsonMapper.writeValueAsString(struct.getValue());
		}
	}

	@SuppressWarnings("rawtypes")
	private String liveExtraMessage(RedisItemReader<?, ?, ?> reader) {
		KeyNotificationItemReader keyReader = (KeyNotificationItemReader) reader.getReader();
		if (keyReader == null || keyReader.getQueue() == null) {
			return "";
		}
		return String.format(QUEUE_MESSAGE, keyReader.getQueue().remainingCapacity(),
				keyReader.count(KeyEventStatus.DROPPED));
	}

	public RedisWriterArgs getTargetRedisWriterArgs() {
		return targetRedisWriterArgs;
	}

	public void setTargetRedisWriterArgs(RedisWriterArgs redisWriterArgs) {
		this.targetRedisWriterArgs = redisWriterArgs;
	}

}
