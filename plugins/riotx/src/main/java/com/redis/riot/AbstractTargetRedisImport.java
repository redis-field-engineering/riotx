package com.redis.riot;

import com.redis.riot.core.RedisContext;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import io.lettuce.core.RedisURI;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractTargetRedisImport extends AbstractImport {

    @ArgGroup(exclusive = false)
    private SingleRedisArgs sourceRedisArgs = new SingleRedisArgs();

    @Option(names = "--target-uri", description = "Target server URI or endpoint in the form host:port. Source endpoint is used if not specified.", paramLabel = "<uri>")
    private RedisURI targetRedisUri;

    @ArgGroup(exclusive = false)
    private TargetRedisArgs targetRedisArgs = new TargetRedisArgs();

    protected RedisContext sourceRedisContext;

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        sourceRedisContext = sourceRedisContext();
        configure(sourceRedisContext);
        sourceRedisContext.afterPropertiesSet();
    }

    @Override
    protected void teardown() {
        if (sourceRedisContext != null) {
            sourceRedisContext.close();
        }
        super.teardown();
    }

    private RedisContext sourceRedisContext() {
        log.info("Creating source Redis context with {}", sourceRedisArgs);
        return sourceRedisArgs.redisContext();
    }

    @Override
    protected RedisContext targetRedisContext() {
        if (targetRedisUri == null) {
            log.info("No target URI specified, using source Redis context for target");
            return sourceRedisContext();
        }
        log.info("Creating target Redis context with {} {}", targetRedisUri, targetRedisArgs);
        return targetRedisArgs.redisContext(targetRedisUri);
    }

    @Override
    protected void configureTarget(RedisItemWriter<?, ?, ?> writer) {
        super.configureTarget(writer);
        writer.setPoolSize(targetRedisArgs.getPoolSize());
    }

    public SingleRedisArgs getSourceRedisArgs() {
        return sourceRedisArgs;
    }

    public void setSourceRedisArgs(SingleRedisArgs sourceRedisArgs) {
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
