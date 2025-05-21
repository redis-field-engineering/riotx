package com.redis.riot;

import com.redis.riot.core.PrefixedNumber;
import com.redis.riot.faker.FakerItemReader;
import com.redis.riot.core.job.StepFactoryBean;
import org.springframework.batch.core.Job;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

@Command(name = "faker", description = "Import Faker data.")
public class FakerImport extends AbstractRedisImport {

    public static final int DEFAULT_COUNT = 1000;

    public static final Locale DEFAULT_LOCALE = Locale.ENGLISH;

    private static final String STEP_NAME = "faker-import-step";

    @Parameters(arity = "1..*", description = "Faker expressions in the form field1=\"exp\" field2=\"exp\" etc. For details see http://www.datafaker.net/documentation/expressions", paramLabel = "EXPRESSION")
    private Map<String, String> fields = new LinkedHashMap<>();

    @Option(names = "--count", description = "Number of items to generate, e.g. 100 10k 5m (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private PrefixedNumber count = PrefixedNumber.of(DEFAULT_COUNT);

    @Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE}).", paramLabel = "<tag>")
    private Locale locale = DEFAULT_LOCALE;

    @Override
    protected Job job() throws Exception {
        StepFactoryBean<Map<String, Object>, Map<String, Object>> step = step(STEP_NAME, reader(), operationWriter());
        step.setItemProcessor(operationProcessor());
        return job(step);
    }

    private FakerItemReader reader() {
        FakerItemReader reader = new FakerItemReader();
        reader.setMaxItemCount(count.intValue());
        reader.setLocale(locale);
        reader.setExpressions(fields);
        return reader;
    }

    public Locale getLocale() {
        return locale;
    }

    public void setLocale(Locale locale) {
        this.locale = locale;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }

    public long getCount() {
        return count.getValue();
    }

    public void setCount(long count) {
        this.count = PrefixedNumber.of(count);
    }

}
