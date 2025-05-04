package com.redis.riot.operation;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import com.redis.spring.batch.item.redis.writer.impl.Zadd;

import io.lettuce.core.ScoredValue;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "zadd", description = "Add members with scores to a sorted set")
public class ZaddCommand extends AbstractMemberOperationCommand {

    @ArgGroup(exclusive = false)
    private ScoreArgs scoreArgs = new ScoreArgs();

    @Override
    public Zadd<String, String, Map<String, Object>> operation() {
        return new Zadd<>(keyFunction(), t -> scoredValues(memberFunction(), score(scoreArgs), t));
    }

    private Collection<ScoredValue<String>> scoredValues(Function<Map<String, Object>, Collection<String>> member,
            ToDoubleFunction<Map<String, Object>> score, Map<String, Object> source) {
        Collection<String> ids = member.apply(source);
        return ids.stream().map(m -> ScoredValue.just(score.applyAsDouble(source), m)).collect(Collectors.toList());
    }

    public ScoreArgs getScoreArgs() {
        return scoreArgs;
    }

    public void setScoreArgs(ScoreArgs scoreArgs) {
        this.scoreArgs = scoreArgs;
    }

}
