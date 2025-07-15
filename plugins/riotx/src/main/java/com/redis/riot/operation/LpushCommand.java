package com.redis.riot.operation;

import java.util.Map;

import com.redis.batch.operation.Lpush;

import org.springframework.expression.EvaluationContext;
import picocli.CommandLine.Command;

@Command(name = "lpush", description = "Insert values at the head of a list")
public class LpushCommand extends AbstractMemberOperationCommand {

    @Override
    public Lpush<byte[], byte[], Map<String, Object>> operation(EvaluationContext context) {
        return new Lpush<>(keyFunction(context), memberFunction(context));
    }

}
