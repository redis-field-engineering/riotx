package com.redis.riot.operation;

import com.redis.batch.BatchUtils;
import com.redis.riot.core.TemplateExpression;
import org.springframework.expression.EvaluationContext;
import picocli.CommandLine;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

abstract class AbstractMemberOperationCommand extends AbstractOperationCommand implements OperationCommand {

    @CommandLine.Option(arity = "1..*", required = true, names = "--member", description = "Member template expression.", paramLabel = "<exp>")
    private List<TemplateExpression> members;

    protected Function<Map<String, Object>, Collection<byte[]>> memberFunction(EvaluationContext context) {
        return t -> members.stream().map(e -> evaluate(e, context, t)).map(BatchUtils.STRING_KEY_TO_BYTES)
                .collect(Collectors.toList());
    }

    public List<TemplateExpression> getMembers() {
        return members;
    }

    public void setMembers(List<TemplateExpression> members) {
        this.members = members;
    }

}
