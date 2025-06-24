package com.redis.riot;

import com.redis.riot.core.RedisContext;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import io.lettuce.core.RedisURI;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Parameters;

public abstract class AbstractRedisTargetExport extends AbstractExport {

    private static final String VAR_TARGET = "target";

    @Parameters(arity = "1", index = "0", defaultValue = "${RIOT_SOURCE}", description = "Source server URI or endpoint in the form host:port.", paramLabel = "SOURCE")
    private RedisURI sourceRedisUri;

    @ArgGroup(exclusive = false)
    private SourceRedisArgs sourceRedisArgs = new SourceRedisArgs();

    @Parameters(arity = "1", index = "1", defaultValue = "${RIOT_TARGET}", description = "Target server URI or endpoint in the form host:port.", paramLabel = "TARGET")
    private RedisURI targetRedisUri;

    @ArgGroup(exclusive = false)
    private TargetRedisArgs targetRedisArgs = new TargetRedisArgs();

    private RedisContext targetRedisContext;

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        targetRedisContext = targetRedisContext();
        targetRedisContext.afterPropertiesSet();
    }

    @Override
    protected void teardown() {
        if (targetRedisContext != null) {
            targetRedisContext.close();
        }
        super.teardown();
    }

    @Override
    protected RedisContext sourceRedisContext() {
        log.info("Creating source Redis context with {} {}", sourceRedisUri, sourceRedisArgs);
        return sourceRedisArgs.redisContext(sourceRedisUri);
    }

    protected RedisContext targetRedisContext() {
        log.info("Creating target Redis context with {} {}", targetRedisUri, targetRedisArgs);
        return targetRedisArgs.redisContext(targetRedisUri);
    }

    @Override
    protected void configure(StandardEvaluationContext context) {
        super.configure(context);
        context.setVariable(VAR_TARGET, targetRedisContext.getConnection().sync());
    }

    protected void configureTarget(RedisItemReader<?, ?, ?> reader) {
        targetRedisContext.configure(reader);
    }

    protected void configureTarget(RedisItemWriter<?, ?, ?> writer) {
        targetRedisContext.configure(writer);
    }

    public RedisURI getSourceRedisUri() {
        return sourceRedisUri;
    }

    public void setSourceRedisUri(RedisURI sourceRedisUri) {
        this.sourceRedisUri = sourceRedisUri;
    }

    public SourceRedisArgs getSourceRedisArgs() {
        return sourceRedisArgs;
    }

    public void setSourceRedisArgs(SourceRedisArgs sourceRedisArgs) {
        this.sourceRedisArgs = sourceRedisArgs;
    }

    public RedisURI getTargetRedisUri() {
        return targetRedisUri;
    }

    public void setTargetRedisUri(RedisURI targetRedisUri) {
        this.targetRedisUri = targetRedisUri;
    }

    public TargetRedisArgs getTargetRedisArgs() {
        return targetRedisArgs;
    }

    public void setTargetRedisArgs(TargetRedisArgs targetRedisArgs) {
        this.targetRedisArgs = targetRedisArgs;
    }

}
