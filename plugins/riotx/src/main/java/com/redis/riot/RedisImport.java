package com.redis.riot;

import com.redis.batch.KeyStructEvent;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.function.KeyValueEventToMap;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.core.job.RiotStep;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import io.lettuce.core.codec.StringCodec;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.Assert;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.regex.Pattern;

@Command(name = "redis-import", description = "Import data from a Redis database.")
public class RedisImport extends AbstractTargetRedisImport {

    private static final String TASK_NAME = "Migrating";

    private static final String STEP_NAME = "redis-import-step";

    @ArgGroup(exclusive = false)
    private RedisReaderArgs sourceRedisReaderArgs = new RedisReaderArgs();

    @Option(names = "--key-regex", description = "Regex for key-field extraction, e.g. '\\w+:(?<id>.+)' extracts an id field from the key", paramLabel = "<rex>")
    private Pattern keyRegex;

    protected void configureSourceRedisReader(RedisScanItemReader<?, ?, ?> reader) {
        sourceRedisContext.configure(reader);
        log.info("Configuring {} with {}", reader.getName(), sourceRedisReaderArgs);
        sourceRedisReaderArgs.configure(reader);
    }

    @Override
    protected Job job() throws Exception {
        Assert.isTrue(hasOperations(), "No Redis command specified");
        RiotStep<KeyStructEvent<String, String>, Map<String, Object>> step = step(STEP_NAME, reader(), operationWriter());
        step.setItemProcessor(RiotUtils.processor(keyValueEventProcessor(), operationProcessor()));
        return job(step);
    }

    private RedisScanItemReader<String, String, KeyStructEvent<String, String>> reader() {
        log.info("Creating source Redis reader with {}", sourceRedisReaderArgs);
        RedisScanItemReader<String, String, KeyStructEvent<String, String>> reader = RedisScanItemReader.struct(
                StringCodec.UTF8);
        configureSourceRedisReader(reader);
        return reader;
    }

    protected ItemProcessor<KeyStructEvent<String, String>, Map<String, Object>> keyValueEventProcessor() {
        KeyValueEventToMap mapFunction = new KeyValueEventToMap();
        if (keyRegex != null) {
            mapFunction.setKey(new RegexNamedGroupFunction(keyRegex));
        }
        return new FunctionItemProcessor<>(mapFunction);
    }

    public RedisReaderArgs getSourceRedisReaderArgs() {
        return sourceRedisReaderArgs;
    }

    public void setSourceRedisReaderArgs(RedisReaderArgs sourceRedisReaderArgs) {
        this.sourceRedisReaderArgs = sourceRedisReaderArgs;
    }

    public Pattern getKeyRegex() {
        return keyRegex;
    }

    public void setKeyRegex(Pattern keyRegex) {
        this.keyRegex = keyRegex;
    }

}
