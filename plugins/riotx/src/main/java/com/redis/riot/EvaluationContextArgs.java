package com.redis.riot;

import com.redis.batch.BatchUtils;
import com.redis.lettucemod.search.GeoLocation;
import com.redis.riot.core.Expression;
import lombok.ToString;
import net.datafaker.Faker;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.CollectionUtils;
import picocli.CommandLine.Option;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

@ToString
public class EvaluationContextArgs {

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    public static final String DEFAULT_NUMBER_FORMAT = "#,###.##";

    public static final String VAR_DATE = "date";

    public static final String VAR_NUMBER = "number";

    public static final String VAR_FAKER = "faker";

    public static final String FUNCTION_GEO = "geo";

    public static final String FUNCTION_FLOAT_ARRAY_TO_BYTES = "floatsToBytes";

    @Option(arity = "1..*", names = "--var", description = "SpEL expressions for context variables, in the form var=\"exp\". For details see https://docs.spring.io/spring-framework/reference/core/expressions.html", paramLabel = "<v=exp>")
    private Map<String, Expression> varExpressions = new LinkedHashMap<>();

    @Option(names = "--date-format", description = "Date/time format (default: ${DEFAULT-VALUE}). For details see https://www.baeldung.com/java-simple-date-format#date_time_patterns", paramLabel = "<fmt>")
    private String dateFormat = DEFAULT_DATE_FORMAT;

    @Option(names = "--number-format", description = "Number format (default: ${DEFAULT-VALUE}). For details see https://www.baeldung.com/java-decimalformat", paramLabel = "<fmt>")
    private String numberFormat = DEFAULT_NUMBER_FORMAT;

    private Map<String, Object> vars = new LinkedHashMap<>();

    public StandardEvaluationContext evaluationContext() {
        StandardEvaluationContext context = new StandardEvaluationContext();
        try {
            context.registerFunction(FUNCTION_GEO, GeoLocation.class.getDeclaredMethod("toString", String.class, String.class));
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException("Could not register function " + FUNCTION_GEO, e);
        }
        try {
            context.registerFunction(FUNCTION_FLOAT_ARRAY_TO_BYTES,
                    BatchUtils.class.getDeclaredMethod("toByteArray", float[].class));
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException("Could not register function " + FUNCTION_FLOAT_ARRAY_TO_BYTES, e);
        }
        context.setVariable(VAR_DATE, new SimpleDateFormat(dateFormat));
        context.setVariable(VAR_NUMBER, new DecimalFormat(numberFormat));
        context.setVariable(VAR_FAKER, new Faker());
        if (!CollectionUtils.isEmpty(vars)) {
            vars.forEach(context::setVariable);
        }
        if (!CollectionUtils.isEmpty(varExpressions)) {
            varExpressions.forEach((k, v) -> context.setVariable(k, v.getValue(context)));
        }
        return context;
    }

    private void registerFunction(StandardEvaluationContext context, String functionName, Class<?> clazz, String methodName,
            Class<?>... parameterTypes) {

    }

    public Map<String, Expression> getVarExpressions() {
        return varExpressions;
    }

    public void setVarExpressions(Map<String, Expression> expressions) {
        this.varExpressions = expressions;
    }

    public Map<String, Object> getVars() {
        return vars;
    }

    public void setVars(Map<String, Object> variables) {
        this.vars = variables;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getNumberFormat() {
        return numberFormat;
    }

    public void setNumberFormat(String numberFormat) {
        this.numberFormat = numberFormat;
    }

}
