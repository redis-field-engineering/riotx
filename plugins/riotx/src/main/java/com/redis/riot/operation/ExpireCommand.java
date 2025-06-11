package com.redis.riot.operation;

import com.redis.batch.RedisOperation;
import com.redis.batch.operation.AbstractWriteOperation;
import com.redis.batch.operation.Expire;
import com.redis.batch.operation.ExpireAt;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.time.Instant;
import java.util.Map;
import java.util.function.ToLongFunction;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractOperationCommand {

    @ArgGroup(exclusive = true)
    private ExpireTtlArgs ttlArgs = new ExpireTtlArgs();

    @Override
    public RedisOperation<byte[], byte[], Map<String, Object>, Object> operation() {
        if (ttlArgs.getTimeField() != null || ttlArgs.getTime() != null) {
            ExpireAt<byte[], byte[], Map<String, Object>> operation = new ExpireAt<>(keyFunction());
            if (ttlArgs.getTime() == null) {
                ToLongFunction<Map<String, Object>> longFunction = toLong(ttlArgs.getTimeField());
                operation.setTimestampFunction(t -> Instant.ofEpochMilli(longFunction.applyAsLong(t)));
            } else {
                operation.setTimestamp(ttlArgs.getTime());
            }
            return operation;
        }
        Expire<byte[], byte[], Map<String, Object>> operation = new Expire<>(keyFunction());
        if (ttlArgs.getTtl() == null) {
            operation.setTtlFunction(toLong(ttlArgs.getTtlField()));
        } else {
            operation.setTtl(ttlArgs.getTtl().toMillis());
        }
        return operation;
    }

    public ExpireTtlArgs getTtlArgs() {
        return ttlArgs;
    }

    public void setTtlArgs(ExpireTtlArgs expireArgs) {
        this.ttlArgs = expireArgs;
    }

}
