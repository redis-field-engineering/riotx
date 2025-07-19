package com.redis.riot;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.riot.core.RedisContext;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisCommand extends AbstractJobCommand {

    @ArgGroup(exclusive = false, heading = "Redis options%n")
    private SingleRedisArgs redisArgs = new SingleRedisArgs();

    private RedisContext redisContext;

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        redisContext = redisArgs.redisContext();
        configure(redisContext);
        redisContext.afterPropertiesSet();
    }

    protected RedisContext getRedisContext() {
        return redisContext;
    }

    @Override
    protected void teardown() {
        if (redisContext != null) {
            redisContext.close();
        }
        super.teardown();
    }

    protected RedisModulesCommands<String, String> commands() {
        return redisContext.getConnection().sync();
    }

    protected void configure(RedisScanItemReader<?, ?, ?> reader) {
        redisContext.configure(reader);
    }

    protected void configure(RedisItemWriter<?, ?, ?> writer) {
        redisContext.configure(writer);
    }

    public SingleRedisArgs getRedisArgs() {
        return redisArgs;
    }

    public void setRedisArgs(SingleRedisArgs clientArgs) {
        this.redisArgs = clientArgs;
    }

}
