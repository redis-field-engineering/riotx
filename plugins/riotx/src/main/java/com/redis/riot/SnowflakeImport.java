package com.redis.riot;

import com.redis.riot.core.OffsetStore;
import com.redis.riot.core.RedisStringOffsetStore;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.RiotVersion;
import com.redis.riot.core.job.StepFactoryBean;
import com.redis.riot.db.DatabaseObject;
import com.redis.riot.db.SnowflakeStreamItemReader;
import com.redis.riot.db.SnowflakeStreamRow;
import com.redis.riot.rdi.ChangeEvent;
import com.redis.riot.rdi.ChangeEventToStreamMessage;
import com.redis.riot.rdi.ChangeEventValue;
import com.redis.riot.rdi.RdiOffsetStore;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.writer.impl.Xadd;
import com.redis.spring.batch.step.FlushingChunkProvider;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.StringCodec;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(name = "snowflake-import", description = "Import from a snowflake table (uses Snowflake Streams to track changes).")
public class SnowflakeImport extends AbstractRedisImport {

    public static final String SNOWFLAKE_DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver";

    public static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;

    public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

    public static final SnowflakeStreamItemReader.SnapshotMode DEFAULT_SNAPSHOT_MODE = SnowflakeStreamItemReader.SnapshotMode.INITIAL;

    private static final String STEP_NAME = "snowflake-import";

    private static final String RDI_STEP_NAME = "rdi-forward";

    private static final Object RDI_STREAM_PREFIX = "rdi";

    public static final String DEFAULT_OFFSET_PREFIX = "riotx:offset";

    public static final String DEFAULT_OFFSET_KEY = RdiOffsetStore.DEFAULT_KEY;

    @ArgGroup(exclusive = false)
    private DataSourceArgs dataSourceArgs = new DataSourceArgs();

    @ArgGroup(exclusive = false)
    private DatabaseReaderArgs readerArgs = new DatabaseReaderArgs();

    @Parameters(arity = "1", description = "Fully qualified Snowflake Table or Materialized View, eg: DB.SCHEMA.TABLE", paramLabel = "TABLE")
    private DatabaseObject table;

    @Option(names = "--snapshot", description = "Snapshot mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
    private SnowflakeStreamItemReader.SnapshotMode snapshotMode = DEFAULT_SNAPSHOT_MODE;

    @Option(names = "--role", description = "Snowflake role to use", paramLabel = "<str>")
    private String role;

    @Option(names = "--warehouse", description = "Snowflake warehouse to use", paramLabel = "<str>")
    private String warehouse;

    @Option(names = "--cdc-database", description = "Snowflake CDC database to use for stream and temp table", paramLabel = "<str>")
    private String cdcDatabase;

    @Option(names = "--cdc-schema", description = "Snowflake CDC schema to use for stream and temp table", paramLabel = "<str>")
    private String cdcSchema;

    @Option(names = "--poll", description = "Snowflake stream polling interval (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration pollInterval = SnowflakeStreamItemReader.DEFAULT_POLL_INTERVAL;

    @Option(names = "--flush-interval", description = "Max duration between batch flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

    @Option(names = "--idle-timeout", description = "Min duration to consider reader complete, for example 3s 5m (default: no timeout).", paramLabel = "<dur>")
    private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

    @Option(names = "--offset-prefix", description = "Key prefix for offset stored in Redis (default: ${DEFAULT-VALUE}", paramLabel = "<str>")
    private String offsetPrefix = DEFAULT_OFFSET_PREFIX;

    @Option(names = "--offset-key", description = "Key name for Debezium offset (default: ${DEFAULT-VALUE}", paramLabel = "<name>")
    private String offsetKey = DEFAULT_OFFSET_KEY;

    @Override
    protected <I, O> StepFactoryBean<I, O> step(String name, ItemReader<I> reader, ItemWriter<O> writer) {
        StepFactoryBean<I, O> step = super.step(name, reader, writer);
        step.setFlushInterval(flushInterval);
        step.setIdleTimeout(idleTimeout);
        return step;
    }

    @Override
    protected Job job() throws Exception {
        return job(step());
    }

    private StepFactoryBean<?, ?> step() {
        if (hasOperations()) {
            SnowflakeStreamItemReader reader = reader();
            reader.setOffsetStore(stringOffsetStore());
            ItemProcessor<SnowflakeStreamRow, Map<String, Object>> processor = RiotUtils.processor(new RowFilter(),
                    new FunctionItemProcessor<>(SnowflakeStreamRow::getColumns), operationProcessor());
            StepFactoryBean<SnowflakeStreamRow, Map<String, Object>> step = step(STEP_NAME, reader, operationWriter());
            step.setItemProcessor(processor);
            return step;
        }
        SnowflakeStreamItemReader reader = reader();
        reader.setOffsetStore(rdiOffsetStore());
        RedisItemWriter<String, String, StreamMessage<String, String>> writer = rdiWriter();
        configureTarget(writer);
        ItemProcessor<SnowflakeStreamRow, StreamMessage<String, String>> processor = RiotUtils.processor(new RowFilter(),
                new FunctionItemProcessor<>(this::changeEvent), new ChangeEventToStreamMessage(rdiStream()));
        StepFactoryBean<SnowflakeStreamRow, StreamMessage<String, String>> step = step(STEP_NAME, reader, writer);
        step.setItemProcessor(processor);
        return step;
    }

    private OffsetStore rdiOffsetStore() {
        RdiOffsetStore store = new RdiOffsetStore(targetRedisContext.client());
        store.setKey(offsetKey);
        return store;
    }

    private OffsetStore stringOffsetStore() {
        String key = String.format("%s:%s", offsetPrefix, table.fullName());
        return new RedisStringOffsetStore(targetRedisContext.client(), key);
    }

    private static class RowFilter implements ItemProcessor<SnowflakeStreamRow, SnowflakeStreamRow> {

        @Override
        public SnowflakeStreamRow process(SnowflakeStreamRow row) throws Exception {
            // Snowflake creates 2 rows for each update: one INSERT and one DELETE. Both have IS_UPDATE=true
            if (row.getAction() == SnowflakeStreamRow.Action.DELETE && row.isUpdate()) {
                // Filter out delete events for updates
                return null;
            }
            return row;
        }

    }

    private ChangeEvent changeEvent(SnowflakeStreamRow row) {
        ChangeEvent event = new ChangeEvent();
        event.setKey(row.getColumns());
        event.setValue(changeEventValue(row));
        return event;
    }

    private ChangeEventValue changeEventValue(SnowflakeStreamRow row) {
        ChangeEventValue value = new ChangeEventValue();
        value.setAfter(row.getColumns());
        value.setOp(operation(row));
        Instant instant = Instant.now();
        value.setTs_ms(instant.toEpochMilli());
        value.setTs_us(micros(instant));
        value.setTs_ns(nanos(instant));
        value.setSource(source());
        return value;
    }

    private long nanos(Instant instant) {
        return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
    }

    private long micros(Instant instant) {
        return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
    }

    private ChangeEventValue.Source source() {
        ChangeEventValue.Source source = new ChangeEventValue.Source();
        source.setConnector("snowflake");
        source.setVersion(RiotVersion.getVersion());
        source.setName("RIOT-X");
        Instant instant = Instant.now();
        source.setTs_ms(instant.toEpochMilli());
        source.setTs_us(micros(instant));
        source.setTs_ns(nanos(instant));
        source.setDb(table.getDatabase());
        source.setTable(table.getTable());
        source.setSchema(table.getSchema());
        return source;
    }

    private ChangeEventValue.Operation operation(SnowflakeStreamRow row) {
        switch (row.getAction()) {
            case INSERT:
                if (row.isUpdate()) {
                    return ChangeEventValue.Operation.UPDATE;
                }
                return ChangeEventValue.Operation.CREATE;
            case DELETE:
                return ChangeEventValue.Operation.DELETE;
            default:
                throw new IllegalArgumentException("Unknown action: " + row.getAction());
        }
    }

    private RedisItemWriter<String, String, StreamMessage<String, String>> rdiWriter() {
        String stream = rdiStream();
        return new RedisItemWriter<>(StringCodec.UTF8, new Xadd<>(m -> stream, Arrays::asList));
    }

    private String rdiStream() {
        return String.format("%s:%s", RDI_STREAM_PREFIX, table.fullName());
    }

    protected SnowflakeStreamItemReader reader() {
        SnowflakeStreamItemReader reader = new SnowflakeStreamItemReader();
        reader.setReaderOptions(readerArgs.readerOptions());
        reader.setStreamDatabase(cdcDatabase);
        reader.setStreamSchema(cdcSchema);
        reader.setDataSource(dataSourceArgs.dataSourceBuilder().driver(SNOWFLAKE_DRIVER).build());
        reader.setPollInterval(pollInterval);
        reader.setRole(role);
        reader.setWarehouse(warehouse);
        reader.setSnapshotMode(snapshotMode);
        reader.setTable(table.fullName());
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

    public DatabaseObject getTable() {
        return table;
    }

    public void setTable(DatabaseObject table) {
        this.table = table;
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

    public String getOffsetKey() {
        return offsetKey;
    }

    public void setOffsetKey(String offsetKey) {
        this.offsetKey = offsetKey;
    }

    public String getOffsetPrefix() {
        return offsetPrefix;
    }

    public void setOffsetPrefix(String offsetPrefix) {
        this.offsetPrefix = offsetPrefix;
    }

}

