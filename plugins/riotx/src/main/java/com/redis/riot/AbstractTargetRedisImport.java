package com.redis.riot;

import java.util.Map;

import com.redis.riot.core.RedisContext;
import com.redis.riot.core.RedisContextFactory;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import io.lettuce.core.RedisURI;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractTargetRedisImport extends AbstractImport {

    @ArgGroup(exclusive = false)
    private RedisArgs sourceRedisArgs = new RedisArgs();

    @Option(names = "--target-uri", description = "Target server URI or endpoint in the form host:port. Source endpoint is used if not specified.", paramLabel = "<uri>")
    private RedisURI targetRedisUri;

    @ArgGroup(exclusive = false)
    private TargetRedisArgs targetRedisArgs = new TargetRedisArgs();

    protected RedisContext sourceRedisContext;

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        sourceRedisContext = sourceRedisContext();
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
        return RedisContextFactory.create(sourceRedisArgs.getUri(), sourceRedisArgs);
    }

    @Override
    protected RedisContext targetRedisContext() {
        if (targetRedisUri == null) {
            log.info("No target URI specified, using source Redis context for target");
            return sourceRedisContext();
        }
        log.info("Creating target Redis context with {} {}", targetRedisUri, targetRedisArgs);
        return RedisContextFactory.create(targetRedisUri, targetRedisArgs);
    }

    @Override
    protected void configureTargetRedisWriter(RedisItemWriter<?, ?, ?> writer) {
        super.configureTargetRedisWriter(writer);
        writer.setPoolSize(targetRedisArgs.getPoolSize());
    }

    @Override
    protected RedisItemWriter<String, String, Map<String, Object>> operationWriter() {
        RedisItemWriter<String, String, Map<String, Object>> writer = super.operationWriter();
        configureTargetRedisWriter(writer);
        return writer;
    }

    public RedisArgs getSourceRedisArgs() {
        return sourceRedisArgs;
    }

    public void setSourceRedisArgs(RedisArgs sourceRedisArgs) {
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
