package com.redis.riot.operation;

import com.redis.lettucemod.search.Suggestion;
import com.redis.riot.core.TemplateExpression;
import com.redis.spring.batch.item.redis.writer.impl.Sugadd;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

@Command(name = "ft.sugadd", description = "Add suggestion strings to a RediSearch auto-complete dictionary")
public class SugaddCommand extends AbstractOperationCommand {

    @Option(names = "--value", required = true, description = "Template expression for the suggestion to add.", paramLabel = "<exp>")
    private TemplateExpression string;

    @Option(names = "--payload", description = "Template expression for the payload.", paramLabel = "<exp>")
    private TemplateExpression payload;

    @Option(names = "--increment", description = "Increment the existing suggestion by the score instead of replacing the score.")
    private boolean increment;

    @ArgGroup(exclusive = false)
    private ScoreArgs scoreArgs = new ScoreArgs();

    @Override
    public Sugadd<String, String, Map<String, Object>> operation() {
        Sugadd<String, String, Map<String, Object>> operation = new Sugadd<>(keyFunction(), suggestion());
        operation.setIncr(increment);
        return operation;
    }

    private Function<Map<String, Object>, Suggestion<String>> suggestion() {
        ToDoubleFunction<Map<String, Object>> score = score(scoreArgs);
        return t -> suggestion(evaluate(string, t), score.applyAsDouble(t), evaluate(payload, t));
    }

    private Suggestion<String> suggestion(String string, double score, String payload) {
        Suggestion<String> suggestion = new Suggestion<>();
        suggestion.setString(string);
        suggestion.setScore(score);
        suggestion.setPayload(payload);
        return suggestion;
    }

    public boolean isIncrement() {
        return increment;
    }

    public void setIncrement(boolean increment) {
        this.increment = increment;
    }

    public ScoreArgs getScoreArgs() {
        return scoreArgs;
    }

    public void setScoreArgs(ScoreArgs scoreArgs) {
        this.scoreArgs = scoreArgs;
    }

}
