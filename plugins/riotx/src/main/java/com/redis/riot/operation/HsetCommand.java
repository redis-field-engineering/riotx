package com.redis.riot.operation;

import java.util.Map;

import com.redis.batch.operation.Hset;

import org.springframework.expression.EvaluationContext;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "hset", description = "Set hashes from input")
public class HsetCommand extends AbstractOperationCommand {

    @ArgGroup(exclusive = false)
    private FieldFilterArgs fieldFilterArgs = new FieldFilterArgs();

    @Override
    public Hset<byte[], byte[], Map<String, Object>> operation(EvaluationContext context) {
        return new Hset<>(keyFunction(context), fieldFilterArgs.mapFunction());
    }

    public FieldFilterArgs getFieldFilterArgs() {
        return fieldFilterArgs;
    }

    public void setFieldFilterArgs(FieldFilterArgs filteringArgs) {
        this.fieldFilterArgs = filteringArgs;
    }

}
