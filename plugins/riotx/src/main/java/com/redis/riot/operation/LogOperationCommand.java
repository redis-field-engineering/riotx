package com.redis.riot.operation;

import com.redis.batch.RedisBatchOperation;
import com.redis.riot.BaseCommand;
import com.redis.riot.core.TemplateExpression;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.springframework.expression.EvaluationContext;
import picocli.CommandLine;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

@CommandLine.Command(name = "log", description = "Print records")
public class LogOperationCommand extends BaseCommand implements OperationCommand {

    @CommandLine.Parameters(arity = "0..1", description = "Template expression to log. If none specified, logs the whole record.", paramLabel = "EXP")
    private TemplateExpression expression;

    @Override
    public RedisBatchOperation<byte[], byte[], Map<String, Object>, Object> operation(EvaluationContext context) {
        if (expression == null) {
            return new SimpleLogOperation();
        }
        return new TemplateLogOperation(context, expression);
    }

    public TemplateExpression getExpression() {
        return expression;
    }

    public void setExpression(TemplateExpression expression) {
        this.expression = expression;
    }

    private static class TemplateLogOperation implements RedisBatchOperation<byte[], byte[], Map<String, Object>, Object> {

        private final EvaluationContext evaluationContext;

        private final TemplateExpression expression;

        private TemplateLogOperation(EvaluationContext evaluationContext, TemplateExpression expression) {
            this.evaluationContext = evaluationContext;
            this.expression = expression;
        }

        @Override
        public List<? extends Future<Object>> execute(RedisAsyncCommands<byte[], byte[]> commands,
                List<? extends Map<String, Object>> items) {
            for (Map<String, Object> item : items) {
                System.out.println(expression.getValue(evaluationContext, item));
            }
            return Collections.emptyList();
        }

    }

    private static class SimpleLogOperation implements RedisBatchOperation<byte[], byte[], Map<String, Object>, Object> {

        @Override
        public List<? extends Future<Object>> execute(RedisAsyncCommands<byte[], byte[]> commands,
                List<? extends Map<String, Object>> items) {
            for (Map<String, Object> item : items) {
                System.out.println(item);
            }
            return Collections.emptyList();
        }

    }

}
