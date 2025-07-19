package com.redis.riot.operation;

import com.redis.batch.BatchUtils;
import com.redis.batch.RedisBatchOperation;
import com.redis.batch.operation.Aggregate;
import com.redis.batch.operation.Search;
import com.redis.riot.BaseCommand;
import com.redis.riot.core.TemplateExpression;
import org.springframework.expression.EvaluationContext;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Command(name = "aggregate", description = "Execute FT.AGGREGATE aggregations")
public class AggregateCommand extends BaseCommand implements OperationCommand {

    @CommandLine.Parameters(arity = "1", index = "0", description = "Index template expression.", paramLabel = "INDEX")
    private TemplateExpression index;

    @CommandLine.Parameters(arity = "1", index = "1", description = "Query template expression.", paramLabel = "QUERY")
    private TemplateExpression query;

    @CommandLine.Parameters(arity = "0..*", index = "2..*", description = "Aggregate options expressions, e.g. groupby 1 @category reduce avg 1 @score as score sortby 2 @score desc.", paramLabel = "OPTIONS")
    private List<TemplateExpression> options = new ArrayList<>();

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public RedisBatchOperation<byte[], byte[], Map<String, Object>, Object> operation(EvaluationContext context) {
        Aggregate<byte[], byte[], Map<String, Object>> aggregate = new Aggregate<>(
                index(context).andThen(BatchUtils.STRING_KEY_TO_BYTES),
                query(context).andThen(BatchUtils.STRING_VALUE_TO_BYTES));
        aggregate.setStringOptions(t -> options.stream().map(e -> e.getValue(context, t)).map(BatchUtils.STRING_VALUE_TO_BYTES)
                .collect(Collectors.toList()));
        return (RedisBatchOperation) aggregate;
    }

    private Function<Map<String, Object>, String> index(EvaluationContext context) {
        return t -> index.getValue(context, t);
    }

    private Function<Map<String, Object>, String> query(EvaluationContext context) {
        return t -> query.getValue(context, t);
    }

    public TemplateExpression getIndex() {
        return index;
    }

    public void setIndex(TemplateExpression index) {
        this.index = index;
    }

    public TemplateExpression getQuery() {
        return query;
    }

    public void setQuery(TemplateExpression query) {
        this.query = query;
    }

    public List<TemplateExpression> getOptions() {
        return options;
    }

    public void setOptions(List<TemplateExpression> options) {
        this.options = options;
    }

}
