package com.redis.riot;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.redis.riot.core.job.RiotStep;
import com.redis.riot.core.RiotUtils;
import org.springframework.batch.core.Job;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyEventItemReader;
import com.redis.spring.batch.item.redis.reader.RedisLiveItemReader;
import com.redis.spring.batch.item.redis.writer.impl.Xadd;

import io.lettuce.core.StreamMessage;
import org.springframework.batch.item.ItemProcessor;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "stream-export", description = "Export Redis data to a Redis stream.")
public class StreamExport extends AbstractRedisTargetExport {

    public static final String DEFAULT_STREAM = "stream:export";

    private static final String QUEUE_MESSAGE = " | capacity: %,d";

    private static final String TASK_NAME = "Exporting";

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
    protected Job job() {
        return job(step());
    }

    @Override
    protected <K, V, T> RedisItemWriter<K, V, T> configureTarget(RedisItemWriter<K, V, T> writer) {
        log.info("Configuring target Redis writer with {}", targetRedisWriterArgs);
        targetRedisWriterArgs.configure(writer);
        return super.configureTarget(writer);
    }

    private RiotStep<KeyValue<String>, StreamMessage<String, String>> step() {
        RedisLiveItemReader<String, String> reader = RedisItemReader.liveStruct();
        configureSource(reader);
        RedisItemWriter<String, String, StreamMessage<String, String>> writer = writer();
        configureTarget(writer);
        RiotStep<KeyValue<String>, StreamMessage<String, String>> step = step("stream-export", reader, writer);
        ItemProcessor<KeyValue<String>, StreamMessage<String, String>> processor = this::process;
        step.processor(RiotUtils.processor(keyValueFilter(), processor));
        return step;
    }

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return TASK_NAME;
    }

    @Override
    protected <I, O> Supplier<String> extraMessage(RiotStep<I, O> step) {
        return () -> liveExtraMessage((RedisLiveItemReader<?, ?>) step.getReader());
    }

    private RedisItemWriter<String, String, StreamMessage<String, String>> writer() {
        return RedisItemWriter.operation(new Xadd<>(t -> stream, Arrays::asList));
    }

    private StreamMessage<String, String> process(KeyValue<String> struct) throws JsonProcessingException {
        Map<String, String> body = new LinkedHashMap<>();
        body.put("key", struct.getKey());
        body.put("time", String.valueOf(struct.getTimestamp()));
        body.put("type", struct.getType());
        body.put("ttl", String.valueOf(struct.getTtl()));
        body.put("mem", String.valueOf(struct.getMemoryUsage()));
        body.put("value", value(struct));
        return new StreamMessage<>(stream, null, body);
    }

    private String value(KeyValue<String> struct) throws JsonProcessingException {
        if (struct.getType() == null) {
            return null;
        }
        switch (struct.getType()) {
            case KeyValue.TYPE_STRING:
            case KeyValue.TYPE_JSON:
                return (String) struct.getValue();
            default:
                return jsonMapper.writeValueAsString(struct.getValue());
        }
    }

    private String liveExtraMessage(RedisLiveItemReader<?, ?> reader) {
        KeyEventItemReader<?, ?> keyReader = reader.getKeyEventReader();
        if (keyReader == null || keyReader.getQueue() == null) {
            return "";
        }
        return String.format(QUEUE_MESSAGE, keyReader.getQueue().remainingCapacity());
    }

    public RedisWriterArgs getTargetRedisWriterArgs() {
        return targetRedisWriterArgs;
    }

    public void setTargetRedisWriterArgs(RedisWriterArgs redisWriterArgs) {
        this.targetRedisWriterArgs = redisWriterArgs;
    }

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

}
