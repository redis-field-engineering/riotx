package com.redis.riot;

import com.redis.batch.gen.Generator;
import com.redis.batch.operation.Xadd;
import com.redis.riot.core.*;
import com.redis.riot.core.job.RiotStep;
import com.redis.riot.db.DatabaseObject;
import com.redis.riot.db.SnowflakeStreamItemReader;
import com.redis.riot.db.SnowflakeStreamRow;
import com.redis.riot.rdi.ChangeEvent;
import com.redis.riot.rdi.ChangeEventToStreamMessage;
import com.redis.riot.rdi.ChangeEventValue;
import com.redis.riot.rdi.RdiOffsetStore;
import com.redis.spring.batch.item.AbstractCountingItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.StringCodec;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.CollectionUtils;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@CommandLine.Command(name = "snowflake-import", description = "Import from a snowflake table (uses Snowflake Streams to track changes).")
public class SnowflakeImport extends AbstractRedisImport {

    public static final String SNOWFLAKE_DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver";

    public static final SnowflakeStreamItemReader.SnapshotMode DEFAULT_SNAPSHOT_MODE = SnowflakeStreamItemReader.SnapshotMode.INITIAL;

    private static final String STEP_NAME = "snowflake-import";

    public static final String DEFAULT_OFFSET_PREFIX = "riotx:offset:";

    public static final String DEFAULT_OFFSET_KEY = RdiOffsetStore.DEFAULT_KEY;

    private static final String SOURCE_NAME = "riotx";

    private static final String CONNECTOR_NAME = "snowflake";

    private static final String STREAM_FORMAT = "%s%s.%s.%s";

    @ArgGroup(exclusive = false)
    private DataSourceArgs dataSourceArgs = new DataSourceArgs();

    @ArgGroup(exclusive = false)
    private DatabaseReaderArgs readerArgs = new DatabaseReaderArgs();

    @Parameters(arity = "1", defaultValue = "${RIOT_TABLE}", description = "Fully qualified Snowflake Table or Materialized View, eg: DB.SCHEMA.TABLE", paramLabel = "TABLE")
    private DatabaseObject table;

    @ArgGroup(exclusive = false)
    private DebeziumStreamArgs debeziumStreamArgs = new DebeziumStreamArgs();

    @CommandLine.Option(names = "--offset-prefix", defaultValue = "${RIOT_OFFSET_PREFIX:-riotx:offset:}", description = "Key prefix for offset stored in Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
    private String offsetPrefix = DEFAULT_OFFSET_PREFIX;

    @CommandLine.Option(names = "--offset-key", defaultValue = "${RIOT_OFFSET_KEY:-offset}", description = "Key name for Debezium offset (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
    private String offsetKey = DEFAULT_OFFSET_KEY;

    @Option(names = "--snapshot", defaultValue = "${RIOT_SNAPSHOT:-INITIAL}", description = "Snapshot mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
    private SnowflakeStreamItemReader.SnapshotMode snapshotMode = DEFAULT_SNAPSHOT_MODE;

    @Option(arity = "0..*", names = "--key-column", defaultValue = "${RIOT_KEY_COLUMN}", description = "Table column name(s) to use as the key for the CDC event", paramLabel = "<name>")
    private Set<String> keyColumns;

    @Option(arity = "0..*", names = "--gen", defaultValue = "${RIOT_GEN}", description = "Columns to simulate CDC activity for instead of connecting to database.", paramLabel = "<name>")
    private Set<String> genColumns;

    @Option(names = "--count", defaultValue = "${RIOT_COUNT}", description = "Max rows to read (default: no limit).", paramLabel = "<num>")
    private int count;

    @Option(names = "--role", defaultValue = "${RIOT_ROLE}", description = "Snowflake role to use", paramLabel = "<str>")
    private String role;

    @Option(names = "--warehouse", defaultValue = "${RIOT_WAREHOUSE}", description = "Snowflake warehouse to use", paramLabel = "<str>")
    private String warehouse;

    @Option(names = "--cdc-database", defaultValue = "${RIOT_CDC_DATABASE}", description = "Snowflake CDC database to use for stream and temp table", paramLabel = "<str>")
    private String cdcDatabase;

    @Option(names = "--cdc-schema", defaultValue = "${RIOT_CDC_SCHEMA}", description = "Snowflake CDC schema to use for stream and temp table", paramLabel = "<str>")
    private String cdcSchema;

    @Option(names = "--poll", defaultValue = "${RIOT_POLL}", description = "Snowflake stream polling interval (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration pollInterval = SnowflakeStreamItemReader.DEFAULT_POLL_INTERVAL;

    @ArgGroup(exclusive = false)
    private FlushingStepArgs flushingStepArgs = new FlushingStepArgs();

    @Override
    protected void initialize() throws Exception {
        super.initialize();
        register(flushingStepArgs);
    }

    @Override
    protected Job job() throws Exception {
        return job(step());
    }

    private RiotStep<?, ?> step() {
        AbstractCountingItemReader<SnowflakeStreamRow> reader = reader();
        if (count > 0) {
            reader.setMaxItemCount(count);
        }
        if (hasOperations()) {
            ItemProcessor<SnowflakeStreamRow, Map<String, Object>> processor = RiotUtils.processor(new RowFilter(),
                    new FunctionItemProcessor<>(SnowflakeStreamRow::getColumns), operationProcessor());
            RiotStep<SnowflakeStreamRow, Map<String, Object>> step = step(STEP_NAME, reader, operationWriter());
            step.setItemProcessor(processor);
            return step;
        }
        ItemProcessor<SnowflakeStreamRow, StreamMessage<String, String>> processor = RiotUtils.processor(new RowFilter(),
                new FunctionItemProcessor<>(this::changeEvent), new ChangeEventToStreamMessage(rdiStreamKey()));
        RiotStep<SnowflakeStreamRow, StreamMessage<String, String>> step = step(STEP_NAME, reader, rdiWriter());
        step.setItemProcessor(processor);
        return step;
    }

    private ItemWriter<StreamMessage<String, String>> rdiWriter() {
        String streamKey = rdiStreamKey();
        Xadd<String, String, StreamMessage<String, String>> xadd = new Xadd<>(m -> streamKey, Arrays::asList);
        RedisItemWriter<String, String, StreamMessage<String, String>> writer = new RedisItemWriter<>(StringCodec.UTF8, xadd);
        configureTarget(writer);
        if (debeziumStreamArgs.getStreamLimit() > 0) {
            return RiotUtils.writer(backpressureWriter(), writer);
        }
        return writer;
    }

    private BackpressureItemWriter<StreamMessage<String, String>> backpressureWriter() {
        BackpressureItemWriter<StreamMessage<String, String>> writer = new BackpressureItemWriter<>();
        StreamLengthBackpressureStatusSupplier statusSupplier = new StreamLengthBackpressureStatusSupplier(
                targetRedisContext.getConnection(), rdiStreamKey());
        statusSupplier.setLimit(debeziumStreamArgs.getStreamLimit());
        writer.setStatusSupplier(statusSupplier);
        return writer;
    }

    private String rdiStreamKey() {
        return String.format(STREAM_FORMAT, debeziumStreamArgs.getStreamPrefix(), SOURCE_NAME, table.getSchema(),
                table.getTable());
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
        ChangeEvent.Builder event = ChangeEvent.builder();
        event.key(changeEventKey(row));
        event.columns(row.getColumns());
        event.operation(operation(row));
        event.source(SOURCE_NAME);
        event.connector(CONNECTOR_NAME);
        event.database(table.getDatabase());
        event.table(table.getTable());
        event.schema(table.getSchema());
        return event.build();
    }

    private Object changeEventKey(SnowflakeStreamRow row) {
        if (CollectionUtils.isEmpty(keyColumns)) {
            return row.getColumns();
        }
        Map<String, Object> keyMap = new HashMap<>();
        for (String column : keyColumns) {
            keyMap.put(column, row.getColumns().get(column));
        }
        return keyMap;
    }

    private ChangeEventValue.Operation operation(SnowflakeStreamRow row) {
        return switch (row.getAction()) {
            case INSERT -> {
                if (row.isUpdate()) {
                    yield ChangeEventValue.Operation.UPDATE;
                }
                yield ChangeEventValue.Operation.CREATE;
            }
            case DELETE -> ChangeEventValue.Operation.DELETE;
        };
    }

    protected AbstractCountingItemReader<SnowflakeStreamRow> reader() {
        if (!CollectionUtils.isEmpty(genColumns)) {
            return new SnowflakeStreamRowGeneratorItemReader(genColumns);
        }
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
        reader.setOffsetStore(offsetStore());
        return reader;
    }

    private OffsetStore offsetStore() {
        if (hasOperations()) {
            String key = offsetPrefix + table.fullName();
            return new RedisStringOffsetStore(targetRedisContext.client(), key);
        }
        RdiOffsetStore store = new RdiOffsetStore(targetRedisContext.client());
        store.setKey(offsetKey);
        return store;
    }

    private static class SnowflakeStreamRowGeneratorItemReader extends AbstractCountingItemReader<SnowflakeStreamRow> {

        public static final int DEFAULT_COLUMN_WIDTH = 10;

        private final Set<String> columnNames;

        private int columnWidth = DEFAULT_COLUMN_WIDTH;

        private final AtomicLong rowId = new AtomicLong();

        private SnowflakeStreamRowGeneratorItemReader(Set<String> columnNames) {
            this.columnNames = columnNames;
        }

        @Override
        protected SnowflakeStreamRow doRead() throws Exception {
            Map<String, Object> columns = generateColumns();
            SnowflakeStreamRow row = new SnowflakeStreamRow();
            row.setColumns(columns);
            row.setUpdate(false);
            row.setAction(SnowflakeStreamRow.Action.INSERT);
            row.setRowId(String.valueOf(rowId.incrementAndGet()));
            return row;
        }

        private Map<String, Object> generateColumns() {
            Map<String, Object> columns = new HashMap<>();
            for (String columnName : columnNames) {
                columns.put(columnName, Generator.string(columnWidth));
            }
            return columns;
        }

        @Override
        protected void doOpen() {
            // do nothing
        }

        @Override
        protected void doClose() {
            // do nothing
        }

        public int getColumnWidth() {
            return columnWidth;
        }

        public void setColumnWidth(int columnWidth) {
            this.columnWidth = columnWidth;
        }

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

    public DebeziumStreamArgs getDebeziumStreamArgs() {
        return debeziumStreamArgs;
    }

    public void setDebeziumStreamArgs(DebeziumStreamArgs debeziumStreamArgs) {
        this.debeziumStreamArgs = debeziumStreamArgs;
    }

    public String getOffsetPrefix() {
        return offsetPrefix;
    }

    public void setOffsetPrefix(String offsetPrefix) {
        this.offsetPrefix = offsetPrefix;
    }

    public String getOffsetKey() {
        return offsetKey;
    }

    public void setOffsetKey(String offsetKey) {
        this.offsetKey = offsetKey;
    }

    public Set<String> getGenColumns() {
        return genColumns;
    }

    public void setGenColumns(Set<String> columns) {
        this.genColumns = columns;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public FlushingStepArgs getFlushingStepArgs() {
        return flushingStepArgs;
    }

    public void setFlushingStepArgs(FlushingStepArgs flushingStepArgs) {
        this.flushingStepArgs = flushingStepArgs;
    }

    public void setKeyColumns(Set<String> keyColumns) {
        this.keyColumns = keyColumns;
    }

    public Set<String> getKeyColumns() {
        return keyColumns;
    }

}

