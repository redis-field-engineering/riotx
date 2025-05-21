package com.redis.riot;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.function.KeyValueToStreamMessage;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyEventItemReader;
import com.redis.spring.batch.item.redis.reader.RedisLiveItemReader;
import com.redis.spring.batch.item.redis.writer.impl.Xadd;
import com.redis.riot.core.job.StepFactoryBean;
import io.lettuce.core.StreamMessage;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Arrays;
import java.util.function.Supplier;

@Command(name = "stream-export", description = "Export Redis data to a Redis stream.")
public class StreamExport extends AbstractRedisTargetExport {

    public static final String DEFAULT_STREAM = "stream:export";

    private static final String QUEUE_MESSAGE = " | capacity: %,d";

    private static final String TASK_NAME = "Exporting";

    private static final String STEP_NAME = "stream-export-step";

    @ArgGroup(exclusive = false)
    private RedisWriterArgs targetRedisWriterArgs = new RedisWriterArgs();

    @Option(names = "--stream", description = "Target stream key (default: ${DEFAULT-VALUE}).", paramLabel = "<key>")
    private String stream = DEFAULT_STREAM;

    @Override
    protected Job job() throws Exception {
        return job(step());
    }

    @Override
    protected void configureTarget(RedisItemWriter<?, ?, ?> writer) {
        log.info("Configuring target Redis writer with {}", targetRedisWriterArgs);
        targetRedisWriterArgs.configure(writer);
        super.configureTarget(writer);
    }

    private StepFactoryBean<KeyValue<String>, StreamMessage<String, String>> step() {
        RedisLiveItemReader<String, String> reader = RedisLiveItemReader.struct();
        configureSource(reader);
        RedisItemWriter<String, String, StreamMessage<String, String>> writer = writer();
        configureTarget(writer);
        ItemProcessor<KeyValue<String>, StreamMessage<String, String>> processor = RiotUtils.processor(keyValueFilter(),
                new KeyValueToStreamMessage(stream));
        StepFactoryBean<KeyValue<String>, StreamMessage<String, String>> step = step(STEP_NAME, reader, writer);
        step.setItemProcessor(processor);
        return step;
    }

    @Override
    protected String taskName(StepFactoryBean<?, ?> step) {
        return TASK_NAME;
    }

    @Override
    protected Supplier<String> extraMessage(StepFactoryBean<?, ?> step) {
        return () -> liveExtraMessage((RedisLiveItemReader<?, ?>) step.getItemReader());
    }

    private RedisItemWriter<String, String, StreamMessage<String, String>> writer() {
        return RedisItemWriter.operation(new Xadd<>(t -> stream, Arrays::asList));
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
