package com.redis.riot.operation;

import com.redis.batch.BatchUtils;
import com.redis.batch.RedisBatchOperation;
import com.redis.batch.operation.Search;
import com.redis.lettucemod.search.Limit;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.riot.BaseCommand;
import com.redis.riot.core.TemplateExpression;
import org.springframework.expression.EvaluationContext;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Map;
import java.util.function.Function;

@Command(name = "search", description = "Execute FT.SEARCH queries")
public class SearchCommand extends BaseCommand implements OperationCommand {

    @CommandLine.Parameters(arity = "1", description = "Index template expression.", paramLabel = "INDEX")
    private TemplateExpression index;

    @CommandLine.Parameters(arity = "1", description = "Query template expression.", paramLabel = "QUERY")
    private TemplateExpression query;

    @CommandLine.Option(names = "--limit", description = "Search limit.", paramLabel = "<num>")
    private long limit;

    @CommandLine.Option(names = "--offset", description = "Search offset.", paramLabel = "<num>")
    private long offset;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public RedisBatchOperation<byte[], byte[], Map<String, Object>, Object> operation(EvaluationContext context) {
        Search<byte[], byte[], Map<String, Object>> search = new Search<>(
                index(context).andThen(BatchUtils.STRING_KEY_TO_BYTES),
                query(context).andThen(BatchUtils.STRING_VALUE_TO_BYTES));
        SearchOptions<byte[], byte[]> options = new SearchOptions<>();
        if (limit > 0) {
            options.setLimit(new Limit(offset, limit));
        }
        search.setOptions(t -> options);
        return (RedisBatchOperation) search;
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

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

}
