package com.redis.riot;

import com.redis.riot.core.RiotUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import com.redis.riot.core.job.StepFactoryBean;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.Map;

@Command(name = "db-export", description = "Export Redis data to a relational database.")
public class DatabaseExport extends AbstractRedisExport {

    public static final boolean DEFAULT_ASSERT_UPDATES = true;

    private static final String STEP_NAME = "db-export-step";

    @ArgGroup(exclusive = false)
    private DataSourceArgs dataSourceArgs = new DataSourceArgs();

    @Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
    private String sql;

    @Option(names = "--assert-updates", description = "Confirm every insert results in update of at least one row. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
    private boolean assertUpdates = DEFAULT_ASSERT_UPDATES;

    @Override
    protected Job job() throws Exception {
        StepFactoryBean<KeyValue<String>, Map<String, Object>> step = step(STEP_NAME, reader(), writer());
        step.setItemProcessor(RiotUtils.processor(keyValueFilter(), mapProcessor()));
        return job(step);
    }

    private ItemReader<KeyValue<String>> reader() {
        RedisScanItemReader<String, String> reader = RedisScanItemReader.struct();
        configureSource(reader);
        return reader;
    }

    private JdbcBatchItemWriter<Map<String, Object>> writer() {
        Assert.hasLength(sql, "No SQL statement specified");
        log.info("Creating data source with {}", dataSourceArgs);
        log.info("Creating JDBC writer with sql=\"{}\" assertUpdates={}", sql, assertUpdates);
        JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
        builder.itemSqlParameterSourceProvider(NullableSqlParameterSource::new);
        builder.dataSource(dataSourceArgs.dataSourceBuilder().build());
        builder.sql(sql);
        builder.assertUpdates(assertUpdates);
        JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
        writer.afterPropertiesSet();
        return writer;
    }

    private static class NullableSqlParameterSource extends MapSqlParameterSource {

        public NullableSqlParameterSource(@Nullable Map<String, ?> values) {
            super(values);
        }

        @Override
        @Nullable
        public Object getValue(String paramName) {
            if (!hasValue(paramName)) {
                return null;
            }
            return super.getValue(paramName);
        }

    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public boolean isAssertUpdates() {
        return assertUpdates;
    }

    public void setAssertUpdates(boolean assertUpdates) {
        this.assertUpdates = assertUpdates;
    }

    public DataSourceArgs getDataSourceArgs() {
        return dataSourceArgs;
    }

    public void setDataSourceArgs(DataSourceArgs dataSourceArgs) {
        this.dataSourceArgs = dataSourceArgs;
    }

}
