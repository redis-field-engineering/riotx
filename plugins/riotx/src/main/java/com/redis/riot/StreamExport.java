package com.redis.riot;

import com.redis.batch.KeyStructEvent;
import com.redis.batch.operation.Xadd;
import com.redis.riot.core.function.KeyStructEventToStreamMessage;
import com.redis.riot.core.job.RiotStep;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.reader.KeyEventItemReader;
import com.redis.spring.batch.item.redis.reader.RedisLiveItemReader;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.StringCodec;
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

    private RiotStep<KeyStructEvent<String, String>, StreamMessage<String, String>> step() {
        RedisLiveItemReader<String, String, KeyStructEvent<String, String>> reader = RedisLiveItemReader.struct(
                StringCodec.UTF8);
        configureSource(reader);
        RedisItemWriter<String, String, StreamMessage<String, String>> writer = writer();
        configureTarget(writer);
        ItemProcessor<KeyStructEvent<String, String>, StreamMessage<String, String>> processor = new KeyStructEventToStreamMessage(
                stream);
        RiotStep<KeyStructEvent<String, String>, StreamMessage<String, String>> step = step(STEP_NAME, reader, writer);
        step.setItemProcessor(processor);
        return step;
    }

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return TASK_NAME;
    }

    @Override
    protected Supplier<String> extraMessage(RiotStep<?, ?> step) {
        return () -> liveExtraMessage((RedisLiveItemReader<?, ?, ?>) step.getItemReader());
    }

    private RedisItemWriter<String, String, StreamMessage<String, String>> writer() {
        return RedisItemWriter.operation(new Xadd<>(t -> stream, Arrays::asList));
    }

    private String liveExtraMessage(RedisLiveItemReader<?, ?, ?> reader) {
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
