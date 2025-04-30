package com.redis.riot;

import java.util.Map;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

import com.redis.riot.core.job.RiotStep;
import com.redis.riot.core.RiotUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.Assert;

import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.core.function.KeyValueMap;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "redis-import", description = "Import data from a Redis database.")
public class RedisImport extends AbstractTargetRedisImport {

    private static final String TASK_NAME = "Migrating";

    @ArgGroup(exclusive = false)
    private RedisReaderArgs sourceRedisReaderArgs = new RedisReaderArgs();

    @Option(names = "--key-regex", description = "Regex for key-field extraction, e.g. '\\w+:(?<id>.+)' extracts an id field from the key", paramLabel = "<rex>")
    private Pattern keyRegex;

    protected void configureSourceRedisReader(RedisScanItemReader<?, ?> reader) {
        sourceRedisContext.configure(reader);
        log.info("Configuring {} with {}", reader.getName(), sourceRedisReaderArgs);
        sourceRedisReaderArgs.configure(reader);
    }

    @Override
    protected Job job() {
        Assert.isTrue(hasOperations(), "No Redis command specified");
        RedisScanItemReader<String, String> reader = reader();
        RiotStep<KeyValue<String>, Map<String, Object>> step = step("redis-import", reader, operationWriter());
        step.processor(RiotUtils.processor(mapProcessor(), processor()));
        return job(step);
    }

    @Override
    protected <I, O> LongSupplier maxItemCount(RiotStep<I, O> step) {
        return BatchUtils.scanSizeEstimator((RedisScanItemReader<?, ?>) step.getReader());
    }

    private RedisScanItemReader<String, String> reader() {
        log.info("Creating source Redis reader with {}", sourceRedisReaderArgs);
        RedisScanItemReader<String, String> reader = RedisItemReader.scanStruct();
        configureSourceRedisReader(reader);
        return reader;
    }

    protected ItemProcessor<KeyValue<String>, Map<String, Object>> mapProcessor() {
        KeyValueMap mapFunction = new KeyValueMap();
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
