package com.redis.riot.operation;

import com.redis.batch.BatchUtils;
import com.redis.riot.BaseCommand;
import com.redis.riot.core.TemplateExpression;
import com.redis.riot.core.function.FieldExtractorFactory;
import com.redis.riot.core.function.IdFunctionBuilder;
import org.springframework.expression.EvaluationContext;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

@Command
public abstract class AbstractOperationCommand extends BaseCommand implements OperationCommand {

    public static final String DEFAULT_SEPARATOR = IdFunctionBuilder.DEFAULT_SEPARATOR;

    @CommandLine.Parameters(arity = "0..1", description = "Key template expression.", paramLabel = "KEY")
    private TemplateExpression key;

    @Option(names = "--keyspace", description = "Keyspace prefix.", paramLabel = "<str>")
    private String keyspace;

    @Option(names = { "-k", "--key" }, arity = "1..*", description = "Key fields.", paramLabel = "<fields>")
    private List<String> keyFields;

    @Option(names = "--key-separator", description = "Key separator (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
    private String keySeparator = DEFAULT_SEPARATOR;

    @Option(names = "--remove", description = "Remove key or member fields the first time they are used.")
    private boolean removeFields;

    @Option(names = "--ignore-missing", description = "Ignore missing fields.")
    private boolean ignoreMissingFields;

    protected EvaluationContext evaluationContext;

    protected Function<Map<String, Object>, String> toString(String field) {
        if (field == null) {
            return s -> null;
        }
        return fieldExtractorFactory().string(field);
    }

    protected FieldExtractorFactory fieldExtractorFactory() {
        return FieldExtractorFactory.builder().remove(removeFields).nullCheck(!ignoreMissingFields).build();
    }

    protected Function<Map<String, Object>, String> idFunction(String prefix, List<String> fields) {
        return new IdFunctionBuilder().separator(keySeparator).remove(removeFields).prefix(prefix).fields(fields).build();
    }

    protected Function<Map<String, Object>, byte[]> keyFunction() {
        return stringKeyFunction().andThen(BatchUtils.STRING_KEY_TO_BYTES);
    }

    private Function<Map<String, Object>, String> stringKeyFunction() {
        if (key == null) {
            return idFunction(keyspace, keyFields);
        }
        return this::key;
    }

    protected String key(Map<String, Object> map) {
        return evaluate(key, map);
    }

    protected String evaluate(TemplateExpression expression, Map<String, Object> map) {
        return expression.getValue(evaluationContext, map);
    }

    protected ToDoubleFunction<Map<String, Object>> score(ScoreArgs args) {
        return toDouble(args.getField(), args.getDefaultValue());
    }

    protected ToDoubleFunction<Map<String, Object>> toDouble(String field, double defaultValue) {
        if (field == null) {
            return m -> defaultValue;
        }
        return fieldExtractorFactory().doubleField(field, defaultValue);
    }

    protected ToLongFunction<Map<String, Object>> toLong(String field) {
        if (field == null) {
            return m -> 0;
        }
        return fieldExtractorFactory().longField(field);
    }

    @Override
    public void setEvaluationContext(EvaluationContext context) {
        this.evaluationContext = context;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public List<String> getKeyFields() {
        return keyFields;
    }

    public void setKeyFields(String... keys) {
        setKeyFields(Arrays.asList(keys));
    }

    public void setKeyFields(List<String> keyFields) {
        this.keyFields = keyFields;
    }

    public String getKeySeparator() {
        return keySeparator;
    }

    public void setKeySeparator(String keySeparator) {
        this.keySeparator = keySeparator;
    }

    public boolean isRemoveFields() {
        return removeFields;
    }

    public void setRemoveFields(boolean removeFields) {
        this.removeFields = removeFields;
    }

    public boolean isIgnoreMissingFields() {
        return ignoreMissingFields;
    }

    public void setIgnoreMissingFields(boolean ignoreMissingFields) {
        this.ignoreMissingFields = ignoreMissingFields;
    }

}
