package com.redis.riot;

import com.redis.batch.KeyType;
import com.redis.batch.gen.Generator;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.riot.core.PrefixedNumber;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.batch.KeyValue;
import com.redis.spring.batch.item.redis.GeneratorItemReader;
import com.redis.riot.core.job.RiotStep;
import org.springframework.batch.core.Job;
import org.springframework.util.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.util.ArrayList;
import java.util.List;

@Command(name = "gen", aliases = "generate", description = "Generate Redis data structures.")
public class Generate extends AbstractRedisCommand {

    public static final int DEFAULT_COUNT = 1000;

    private static final String TASK_NAME = "Generating";

    private static final String STEP_NAME = "generate-step";

    @CommandLine.Option(names = "--count", description = "Number of items to generate, e.g. 100 10k 5m (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private PrefixedNumber count = PrefixedNumber.of(DEFAULT_COUNT);

    @ArgGroup(exclusive = false)
    private GenerateArgs generateArgs = new GenerateArgs();

    @ArgGroup(exclusive = false, heading = "Redis writer options%n")
    private RedisWriterArgs redisWriterArgs = new RedisWriterArgs();

    @Override
    protected Job job() throws Exception {
        if (StringUtils.hasLength(generateArgs.getIndex())) {
            commands().ftCreate(generateArgs.getIndex(), indexCreateOptions(), indexFields());
        }
        RiotStep<KeyValue<String>, KeyValue<String>> step = step(STEP_NAME, reader(), writer());
        return job(step);
    }

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return TASK_NAME;
    }

    private RedisItemWriter<String, String, KeyValue<String>> writer() {
        RedisItemWriter<String, String, KeyValue<String>> writer = RedisItemWriter.struct();
        configure(writer);
        log.info("Configuring Redis writer with {}", redisWriterArgs);
        redisWriterArgs.configure(writer);
        return writer;
    }

    private CreateOptions<String, String> indexCreateOptions() {
        CreateOptions.Builder<String, String> options = CreateOptions.builder();
        options.on(indexOn());
        options.prefix(generateArgs.getKeyspace() + generateArgs.getKeySeparator());
        return options.build();
    }

    private CreateOptions.DataType indexOn() {
        if (isJson()) {
            return CreateOptions.DataType.JSON;
        }
        return CreateOptions.DataType.HASH;
    }

    private boolean isJson() {
        return generateArgs.getTypes().contains(KeyType.JSON);
    }

    @SuppressWarnings("unchecked")
    private Field<String>[] indexFields() {
        int fieldCount = indexFieldCount();
        List<Field<String>> fields = new ArrayList<>();
        for (int index = 1; index <= fieldCount; index++) {
            fields.add(indexField(index));
        }
        return fields.toArray(new Field[0]);
    }

    private Field<String> indexField(int index) {
        String name = "field" + index;
        if (isJson()) {
            return Field.tag("$." + name).as(name).build();
        }
        return Field.tag(name).build();
    }

    private int indexFieldCount() {
        if (isJson()) {
            return generateArgs.getJsonFieldCount().getMax();
        }
        return generateArgs.getHashFieldCount().getMax();
    }

    private GeneratorItemReader reader() {
        Generator generator = new Generator();
        generateArgs.configure(generator);
        GeneratorItemReader reader = new GeneratorItemReader(generator);
        reader.setMaxItemCount(count.intValue());
        return reader;
    }

    public RedisWriterArgs getRedisWriterArgs() {
        return redisWriterArgs;
    }

    public void setRedisWriterArgs(RedisWriterArgs args) {
        this.redisWriterArgs = args;
    }

    public GenerateArgs getGenerateArgs() {
        return generateArgs;
    }

    public void setGenerateArgs(GenerateArgs args) {
        this.generateArgs = args;
    }

    public long getCount() {
        return count.getValue();
    }

    public void setCount(long count) {
        this.count = PrefixedNumber.of(count);
    }

}
