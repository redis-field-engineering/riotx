package com.redis.riot;

import com.redis.riot.db.JdbcReaderFactory;
import com.redis.riot.db.SnowflakeColumnMapRowMapper;
import com.redis.riot.core.job.RiotStep;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import javax.sql.DataSource;
import java.util.Map;

@Command(name = "db-import", description = "Import from a relational database.")
public class DatabaseImport extends AbstractRedisImport {

    private static final String STEP_NAME = "db-import-step";

    @ArgGroup(exclusive = false)
    private DataSourceArgs dataSourceArgs = new DataSourceArgs();

    @Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
    protected String sql;

    @ArgGroup(exclusive = false)
    private DatabaseReaderArgs readerArgs = new DatabaseReaderArgs();

    @Override
    protected Job job() throws Exception {
        RiotStep<Map<String, Object>, Map<String, Object>> step = step(STEP_NAME, reader(), operationWriter());
        step.setItemProcessor(operationProcessor());
        return job(step);
    }

    protected JdbcCursorItemReader<Map<String, Object>> reader() {
        log.info("Creating JDBC reader with sql=\"{}\" {} {}", sql, dataSourceArgs, readerArgs);
        JdbcCursorItemReaderBuilder<Map<String, Object>> reader = JdbcReaderFactory.create(readerArgs.readerOptions());
        reader.sql(sql);
        reader.name(sql);
        reader.rowMapper(isSnowflake() ? new SnowflakeColumnMapRowMapper() : new ColumnMapRowMapper());
        reader.dataSource(dataSource());
        return reader.build();
    }

    private boolean isSnowflake() {
        return dataSourceArgs.getUrl().toLowerCase().startsWith("jdbc:snowflake");
    }

    private DataSource dataSource() {
        return dataSourceArgs.dataSourceBuilder().build();
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public DatabaseReaderArgs getReaderArgs() {
        return readerArgs;
    }

    public void setReaderArgs(DatabaseReaderArgs args) {
        this.readerArgs = args;
    }

    public DataSourceArgs getDataSourceArgs() {
        return dataSourceArgs;
    }

    public void setDataSourceArgs(DataSourceArgs args) {
        this.dataSourceArgs = args;
    }

}
