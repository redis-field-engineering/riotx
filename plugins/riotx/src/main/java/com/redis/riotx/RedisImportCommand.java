package com.redis.riotx;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.Assert;

import com.redis.riot.ExportStepHelper;
import com.redis.riot.RedisReaderArgs;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.RiotStep;
import com.redis.riot.core.processor.RegexNamedGroupFunction;
import com.redis.riot.function.KeyValueMap;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "redis-import", description = "Import data from a Redis database.")
public class RedisImportCommand extends AbstractTargetRedisImportCommand {

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
        RiotStep<KeyValue<String>, Map<String, Object>> step = new RiotStep<>(reader, operationWriter());
        step.processor(RiotUtils.processor(mapProcessor(), processor()));
        step.taskName(TASK_NAME);
        if (reader.getMode() == ReaderMode.SCAN) {
            step.maxItemCount(reader.scanSizeEstimator());
        } else {
            ExportStepHelper.checkNotifyConfig(reader.getClient(), log);
            log.info("Configuring export step with live true, flushInterval {}, idleTimeout {}", reader.getFlushInterval(),
                    reader.getIdleTimeout());
            step.live(true);
            step.setFlushInterval(reader.getFlushInterval());
            step.idleTimeout(reader.getIdleTimeout());
        }
        return job(step);
    }

    private RedisScanItemReader<String, String> reader() {
        log.info("Creating source Redis reader with {}", sourceRedisReaderArgs);
        RedisScanItemReader<String, String> reader = RedisScanItemReader.struct();
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

}
