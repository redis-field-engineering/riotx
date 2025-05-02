package com.redis.riot;

import com.redis.riot.core.job.RiotStep;
import com.redis.riot.db.SnowflakeStreamItemReader;
import com.redis.spring.batch.step.FlushingChunkProvider;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemReader;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.time.Duration;
import java.util.Map;

@CommandLine.Command(name = "snowflake-import", description = "Import from a snowflake table (uses Snowflake Streams to track changes).")
public class SnowflakeImport extends AbstractRedisImport {

    public static final String SNOWFLAKE_DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver";

    public static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;

    private static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

    @ArgGroup(exclusive = false)
    private DataSourceArgs dataSourceArgs = new DataSourceArgs();

    @ArgGroup(exclusive = false)
    private DatabaseReaderArgs readerArgs = new DatabaseReaderArgs();

    @Parameters(arity = "1", description = "Fully qualified Snowflake Table or Materialized View, eg: DB.SCHEMA.TABLE", paramLabel = "TABLE")
    private String tableOrView;

    @Option(names = "--snapshot-mode", description = "Snapshot mode: ${COMPLETION-CANDIDATES} . INITIAL is the default and will set the Snowflake Stream SHOW_INITIAL_ROWS=TRUE option", defaultValue = "INITIAL")
    private SnowflakeStreamItemReader.SnapshotMode snapshotMode;

    @Option(names = "--role", description = "Snowflake role to use", paramLabel = "<str>")
    private String role;

    @Option(names = "--warehouse", description = "Snowflake warehouse to use", paramLabel = "<str>")
    private String warehouse;

    @Option(names = "--cdc-database", description = "Snowflake CDC database to use for stream and temp table", paramLabel = "<str>")
    private String cdcDatabase;

    @Option(names = "--cdc-schema", description = "Snowflake CDC schema to use for stream and temp table", paramLabel = "<str>")
    private String cdcSchema;

    @Option(names = "--poll", description = "Table polling interval (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration pollInterval = SnowflakeStreamItemReader.DEFAULT_POLL_INTERVAL;

    @Option(names = "--flush-interval", description = "Max duration between batch flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

    @Option(names = "--idle-timeout", description = "Min duration to consider reader complete, for example 3s 5m (default: no timeout).", paramLabel = "<dur>")
    private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

    @Override
    protected Job job() {
        return job(step(reader()));
    }

    @Override
    protected RiotStep<Map<String, Object>, Map<String, Object>> step(ItemReader<Map<String, Object>> reader) {
        RiotStep<Map<String, Object>, Map<String, Object>> step = super.step(reader);
        step.flushInterval(flushInterval);
        step.idleTimeout(idleTimeout);
        return step;
    }

    protected SnowflakeStreamItemReader reader() {
        SnowflakeStreamItemReader reader = new SnowflakeStreamItemReader();
        reader.setRedisClient(targetRedisContext.getClient());
        reader.setReaderOptions(readerArgs.readerOptions());
        reader.setCdcDatabase(cdcDatabase);
        reader.setCdcSchema(cdcSchema);
        reader.setDataSource(dataSourceArgs.dataSourceBuilder().driver(SNOWFLAKE_DRIVER).build());
        reader.setPollInterval(pollInterval);
        reader.setRole(role);
        reader.setWarehouse(warehouse);
        reader.setSnapshotMode(snapshotMode);
        reader.setTableOrView(tableOrView);
        return reader;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public String getCdcSchema() {
        return cdcSchema;
    }

    public void setCdcSchema(String cdcSchema) {
        this.cdcSchema = cdcSchema;
    }

    public String getCdcDatabase() {
        return cdcDatabase;
    }

    public void setCdcDatabase(String cdcDatabase) {
        this.cdcDatabase = cdcDatabase;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public SnowflakeStreamItemReader.SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public void setSnapshotMode(SnowflakeStreamItemReader.SnapshotMode snapshotMode) {
        this.snapshotMode = snapshotMode;
    }

    public DatabaseReaderArgs getReaderArgs() {
        return readerArgs;
    }

    public void setReaderArgs(DatabaseReaderArgs readerArgs) {
        this.readerArgs = readerArgs;
    }

    public String getTableOrView() {
        return tableOrView;
    }

    public void setTableOrView(String tableOrView) {
        this.tableOrView = tableOrView;
    }

    public DataSourceArgs getDataSourceArgs() {
        return dataSourceArgs;
    }

    public void setDataSourceArgs(DataSourceArgs dataSourceArgs) {
        this.dataSourceArgs = dataSourceArgs;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(Duration flushInterval) {
        this.flushInterval = flushInterval;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

}

