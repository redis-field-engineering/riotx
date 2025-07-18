package com.redis.riot;

import com.redis.riot.core.ExpressionProcessor;
import com.redis.riot.core.QuietMapAccessor;
import com.redis.riot.core.RedisContext;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.function.PredicateOperator;
import com.redis.riot.operation.*;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.batch.operation.MultiOperation;
import com.redis.batch.RedisBatchOperation;
import com.redis.riot.core.job.RiotStep;
import io.lettuce.core.codec.ByteArrayCodec;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;
 import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Command(subcommands = { ExpireCommand.class, DelCommand.class, GeoaddCommand.class, HsetCommand.class,
        LogOperationCommand.class, LpushCommand.class, RpushCommand.class, SaddCommand.class, SearchCommand.class,
        SetCommand.class, XaddCommand.class, ZaddCommand.class, SugaddCommand.class, JsonSetCommand.class,
        TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImport extends AbstractJobCommand {

    private static final String TASK_NAME = "Importing";

    public static final String VAR_REDIS = "redis";

    @ArgGroup(exclusive = false)
    private RedisWriterArgs targetRedisWriterArgs = new RedisWriterArgs();

    @ArgGroup(exclusive = false)
    private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

    @ArgGroup(exclusive = false)
    private ImportProcessorArgs processorArgs = new ImportProcessorArgs();

    /**
     * Initialized manually during command parsing
     */
    private List<OperationCommand> importOperationCommands = new ArrayList<>();

    protected RedisContext targetRedisContext;

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return TASK_NAME;
    }

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        targetRedisContext = targetRedisContext();
        configure(targetRedisContext);
        targetRedisContext.afterPropertiesSet();
    }

    @Override
    protected void teardown() {
        if (targetRedisContext != null) {
            targetRedisContext.close();
        }
        super.teardown();
    }

    private List<RedisBatchOperation<byte[], byte[], Map<String, Object>, Object>> operations(
            StandardEvaluationContext evaluationContext) {
        Assert.isTrue(hasOperations(), "No Redis command specified");
        return importOperationCommands.stream().map(c -> c.operation(evaluationContext)).collect(Collectors.toList());
    }

    protected StandardEvaluationContext evaluationContext() {
        log.info("Creating SpEL evaluation context with {}", evaluationContextArgs);
        StandardEvaluationContext evaluationContext = evaluationContextArgs.evaluationContext();
        evaluationContext.setVariable(VAR_REDIS, targetRedisContext.getConnection().sync());
        evaluationContext.addPropertyAccessor(new QuietMapAccessor());
        return evaluationContext;
    }

    protected boolean hasOperations() {
        return !CollectionUtils.isEmpty(importOperationCommands);
    }

    protected ItemProcessor<Map<String, Object>, Map<String, Object>> operationProcessor() {
        return operationProcessor(evaluationContext());
    }

    protected ItemProcessor<Map<String, Object>, Map<String, Object>> operationProcessor(StandardEvaluationContext context) {
        return processor(context, processorArgs);
    }

    protected abstract RedisContext targetRedisContext();

    public static ItemProcessor<Map<String, Object>, Map<String, Object>> processor(EvaluationContext evaluationContext,
            ImportProcessorArgs args) {
        List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
        if (args.getFilter() != null) {
            processors.add(new FunctionItemProcessor<>(new PredicateOperator<>(args.getFilter().predicate(evaluationContext))));
        }
        if (!CollectionUtils.isEmpty(args.getExpressions())) {
            processors.add(new ExpressionProcessor(evaluationContext, args.getExpressions()));
        }
        return RiotUtils.processor(processors);
    }

    protected RedisItemWriter<byte[], byte[], Map<String, Object>> operationWriter() {
        return operationWriter(evaluationContext());
    }

    protected RedisItemWriter<byte[], byte[], Map<String, Object>> operationWriter(
            StandardEvaluationContext evaluationContext) {
        RedisItemWriter<byte[], byte[], Map<String, Object>> writer = new RedisItemWriter<>(ByteArrayCodec.INSTANCE,
                new MultiOperation<>(operations(evaluationContext)));
        configureTarget(writer);
        return writer;
    }

    protected void configureTarget(RedisItemWriter<?, ?, ?> writer) {
        targetRedisContext.configure(writer);
        log.info("Configuring target Redis writer with {}", targetRedisWriterArgs);
        targetRedisWriterArgs.configure(writer);
    }

    public RedisWriterArgs getTargetRedisWriterArgs() {
        return targetRedisWriterArgs;
    }

    public void setTargetRedisWriterArgs(RedisWriterArgs args) {
        this.targetRedisWriterArgs = args;
    }

    public List<OperationCommand> getImportOperationCommands() {
        return importOperationCommands;
    }

    public void setImportOperationCommands(OperationCommand... commands) {
        setImportOperationCommands(Arrays.asList(commands));
    }

    public void setImportOperationCommands(List<OperationCommand> commands) {
        this.importOperationCommands = commands;
    }

    public ImportProcessorArgs getProcessorArgs() {
        return processorArgs;
    }

    public void setProcessorArgs(ImportProcessorArgs args) {
        this.processorArgs = args;
    }

    public EvaluationContextArgs getEvaluationContextArgs() {
        return evaluationContextArgs;
    }

    public void setEvaluationContextArgs(EvaluationContextArgs evaluationContextArgs) {
        this.evaluationContextArgs = evaluationContextArgs;
    }

}
