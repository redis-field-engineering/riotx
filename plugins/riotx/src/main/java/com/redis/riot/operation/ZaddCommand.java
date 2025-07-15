package com.redis.riot.operation;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import com.redis.batch.operation.Zadd;

import io.lettuce.core.ScoredValue;
import org.springframework.expression.EvaluationContext;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "zadd", description = "Add members with scores to a sorted set")
public class ZaddCommand extends AbstractMemberOperationCommand {

    @ArgGroup(exclusive = false)
    private ScoreArgs scoreArgs = new ScoreArgs();

    @Override
    public Zadd<byte[], byte[], Map<String, Object>> operation(EvaluationContext context) {
        return new Zadd<>(keyFunction(context), t -> scoredValues(memberFunction(context), score(scoreArgs), t));
    }

    private Collection<ScoredValue<byte[]>> scoredValues(Function<Map<String, Object>, Collection<byte[]>> member,
            ToDoubleFunction<Map<String, Object>> score, Map<String, Object> source) {
        Collection<byte[]> ids = member.apply(source);
        return ids.stream().map(m -> ScoredValue.just(score.applyAsDouble(source), m)).collect(Collectors.toList());
    }

    public ScoreArgs getScoreArgs() {
        return scoreArgs;
    }

    public void setScoreArgs(ScoreArgs scoreArgs) {
        this.scoreArgs = scoreArgs;
    }

}
