package com.redis.riot;

import com.redis.batch.KeyValueEvent;
import com.redis.riot.core.RedisContext;
import com.redis.riot.core.function.KeyValueEventToMap;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.core.job.RiotStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.regex.Pattern;

public abstract class AbstractRedisExport extends AbstractExport {

    @ArgGroup(exclusive = false, heading = "Redis options%n")
    private SingleRedisArgs redisArgs = new SingleRedisArgs();

    @Option(names = "--key-regex", description = "Regex for key-field extraction, e.g. '\\w+:(?<id>.+)' extracts an id field from the key", paramLabel = "<rex>")
    private Pattern keyRegex;

    private static final String TASK_NAME = "Exporting";

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return TASK_NAME;
    }

    @Override
    protected RedisContext sourceRedisContext() {
        return redisArgs.redisContext();
    }

    protected ItemProcessor<KeyValueEvent<String>, Map<String, Object>> mapProcessor() {
        KeyValueEventToMap mapFunction = new KeyValueEventToMap();
        if (keyRegex != null) {
            mapFunction.setKey(new RegexNamedGroupFunction(keyRegex));
        }
        return new FunctionItemProcessor<>(mapFunction);
    }

    public SingleRedisArgs getRedisArgs() {
        return redisArgs;
    }

    public void setRedisArgs(SingleRedisArgs clientArgs) {
        this.redisArgs = clientArgs;
    }

    public Pattern getKeyRegex() {
        return keyRegex;
    }

    public void setKeyRegex(Pattern regex) {
        this.keyRegex = regex;
    }

}
