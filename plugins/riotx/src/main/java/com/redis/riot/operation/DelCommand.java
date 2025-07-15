package com.redis.riot.operation;

import java.util.Map;

import com.redis.batch.operation.Del;

import org.springframework.expression.EvaluationContext;
import picocli.CommandLine.Command;

@Command(name = "del", description = "Delete keys")
public class DelCommand extends AbstractOperationCommand {

    @Override
    public Del<byte[], byte[], Map<String, Object>> operation(EvaluationContext context) {
        return new Del<>(keyFunction(context));
    }

}
