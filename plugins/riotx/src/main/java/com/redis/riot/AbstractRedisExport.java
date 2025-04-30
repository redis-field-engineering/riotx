package com.redis.riot;

import com.redis.riot.core.RedisContext;
import com.redis.riot.core.RedisContextFactory;
import com.redis.riot.core.job.RiotStep;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.function.KeyValueMap;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.regex.Pattern;

public abstract class AbstractRedisExport extends AbstractExport {

    @ArgGroup(exclusive = false, heading = "Redis options%n")
    private RedisArgs redisArgs = new RedisArgs();

    @Option(names = "--key-regex", description = "Regex for key-field extraction, e.g. '\\w+:(?<id>.+)' extracts an id field from the key", paramLabel = "<rex>")
    private Pattern keyRegex;

    private static final String TASK_NAME = "Exporting";

    protected <T> RiotStep<KeyValue<String>, T> step(ItemProcessor<KeyValue<String>, T> processor, ItemWriter<T> writer) {
        RiotStep<KeyValue<String>, T> step = step("export", RedisItemReader.scanStruct(), writer);
        step.processor(RiotUtils.processor(keyValueFilter(), processor));
        return step;
    }

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return TASK_NAME;
    }

    @Override
    protected RedisContext sourceRedisContext() {
        return RedisContextFactory.create(redisArgs.getUri(), redisArgs);
    }

    protected ItemProcessor<KeyValue<String>, Map<String, Object>> mapProcessor() {
        KeyValueMap mapFunction = new KeyValueMap();
        if (keyRegex != null) {
            mapFunction.setKey(new RegexNamedGroupFunction(keyRegex));
        }
        return new FunctionItemProcessor<>(mapFunction);
    }

    public RedisArgs getRedisArgs() {
        return redisArgs;
    }

    public void setRedisArgs(RedisArgs clientArgs) {
        this.redisArgs = clientArgs;
    }

    public Pattern getKeyRegex() {
        return keyRegex;
    }

    public void setKeyRegex(Pattern regex) {
        this.keyRegex = regex;
    }

}
