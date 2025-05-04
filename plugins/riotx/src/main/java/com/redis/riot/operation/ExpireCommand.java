package com.redis.riot.operation;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.ToLongFunction;

import com.redis.spring.batch.item.redis.writer.impl.AbstractWriteOperation;
import com.redis.spring.batch.item.redis.writer.impl.Expire;
import com.redis.spring.batch.item.redis.writer.impl.ExpireAt;

import org.springframework.expression.EvaluationContext;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractOperationCommand {

    @ArgGroup(exclusive = true)
    private ExpireTtlArgs ttlArgs = new ExpireTtlArgs();

    @Override
    public AbstractWriteOperation<String, String, Map<String, Object>> operation() {
        if (ttlArgs.getTimeField() != null || ttlArgs.getTime() != null) {
            ExpireAt<String, String, Map<String, Object>> operation = new ExpireAt<>(keyFunction());
            if (ttlArgs.getTime() == null) {
                operation.setTimestampFunction(toLong(ttlArgs.getTimeField()));
            } else {
                operation.setTimestamp(ttlArgs.getTime().toEpochMilli());
            }
            return operation;
        }
        Expire<String, String, Map<String, Object>> operation = new Expire<>(keyFunction());
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
