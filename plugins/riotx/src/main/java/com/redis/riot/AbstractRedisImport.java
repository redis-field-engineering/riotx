package com.redis.riot;

import com.redis.riot.core.RedisContext;
import com.redis.riot.core.job.RiotStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisImport extends AbstractImport {

    @ArgGroup(exclusive = false, heading = "Redis options%n")
    private SingleRedisArgs redisArgs = new SingleRedisArgs();

    @Override
    protected RedisContext targetRedisContext() {
        return redisArgs.redisContext();
    }

    public SingleRedisArgs getRedisArgs() {
        return redisArgs;
    }

    public void setRedisArgs(SingleRedisArgs clientArgs) {
        this.redisArgs = clientArgs;
    }

}
