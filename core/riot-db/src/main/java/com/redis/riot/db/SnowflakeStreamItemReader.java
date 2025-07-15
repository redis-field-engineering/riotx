package com.redis.riot.db;

import com.redis.riot.core.InMemoryOffsetStore;
import com.redis.riot.core.OffsetStore;
import com.redis.spring.batch.item.AbstractCountingPollableItemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SnowflakeStreamItemReader extends AbstractCountingPollableItemReader<SnowflakeStreamRow> {

    private static final String OFFSET_SQL = "SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP(?)";

    private static final String STREAM_HAS_DATA_SQL = "SELECT SYSTEM$STREAM_HAS_DATA(?)";

    private static final String CREATE_STREAM_BASE = "CREATE OR REPLACE STREAM %s ON TABLE %s";

    private static final String CREATE_STREAM_INIT = CREATE_STREAM_BASE + " SHOW_INITIAL_ROWS=?";

    private static final String CREATE_STREAM_OFFSET = CREATE_STREAM_BASE + " AT (TIMESTAMP => TO_TIMESTAMP(?))";

    private static final String CREATE_TEMP_TABLE_SQL = "CREATE OR REPLACE TABLE %s AS SELECT * FROM %s";

    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(3);

    private static final String OFFSET_KEY = "offset";

    public enum SnapshotMode {
        INITIAL, NEVER;

    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    private OffsetStore offsetStore = new InMemoryOffsetStore();

    private JdbcReaderOptions readerOptions = new JdbcReaderOptions();

    /**
     * Full table name in the form db.schema.table
     */
    private String table;

    private DataSource dataSource;

    private SnapshotMode snapshotMode;

    /**
     * Snowflake role to use
     */
    private String role;

    /**
     * Snowflake warehouse to use
     */
    private String warehouse;

    /**
     * Database to use for stream. If not specified will use table database name
     */
    private String streamDatabase;

    /**
     * Schema to use for stream. If not specified will use table schema
     */
    private String streamSchema;

    private DataSource initSqlDataSource;

    private JdbcCursorItemReader<SnowflakeStreamRow> reader;

    private DatabaseObject streamObject;

    private String nextOffset;

    private Instant lastFetchTime = Instant.MIN;

    private Duration pollInterval = DEFAULT_POLL_INTERVAL;

    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(dataSource, "DataSource is required");
        if (initSqlDataSource == null) {
            initSqlDataSource = initSqlDataSource();
        }
        if (streamObject == null) {
            DatabaseObject tableObject = DatabaseObject.parse(table);
            streamObject = new DatabaseObject();
            streamObject.setDatabase(streamDatabase == null ? tableObject.getDatabase() : streamDatabase);
            streamObject.setSchema(streamSchema == null ? tableObject.getSchema() : streamSchema);
            streamObject.setName(String.format("%s_changestream", tableObject.getName()));
        }
        if (nextOffset == null) {
            fetch();
            lastFetchTime = Instant.now();
        }
        if (reader == null) {
            try {
                reader = reader();
                reader.open(new ExecutionContext());
            } catch (Exception e) {
                log.error(
                        "Failed to open JDBC reader for temp table '{}'. This usually indicates the temp table was not created successfully",
                        tempTableName(), e);
                throw e;
            }
        }
    }

    private String tempTableName() {
        return String.format("%s_temp", streamObject.fullName());
    }

    private DataSource initSqlDataSource() {
        List<String> initSqlStatements = new ArrayList<>();
        if (role != null) {
            initSqlStatements.add(String.format("USE ROLE %s", sanitize(role)));
        }
        if (warehouse != null) {
            initSqlStatements.add(String.format("USE WAREHOUSE %s", sanitize(warehouse)));
        }
        return new InitSqlDataSource(dataSource, initSqlStatements);
    }

    @Override
    protected void doClose() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        nextOffset = null;
        streamObject = null;
        initSqlDataSource = null;
    }

    @Override
    protected synchronized SnowflakeStreamRow doPoll(long timeout, TimeUnit unit) throws Exception {
        SnowflakeStreamRow item = reader.read();
        if (item == null) {
            if (nextOffset != null) {
                Map<String, Object> offsetMap = new HashMap<>();
                offsetMap.put(OFFSET_KEY, nextOffset);
                offsetStore.store(offsetMap);
                try (Connection sqlConnection = connection();
                        PreparedStatement dropStatement = sqlConnection.prepareStatement(
                                String.format("DROP TABLE %s", tempTableName()))) {
                    dropStatement.execute();
                }
                nextOffset = null;
            }
            if (shouldFetch()) {
                if (streamHasData()) {
                    reader.close();
                    fetch();
                    reader.open(new ExecutionContext());
                    item = reader.read();
                }
                // Always set the fetch time to avoid calling streamHasData repeatedly
                lastFetchTime = Instant.now();
            }
        }
        return item;
    }

    private Connection connection() throws SQLException {
        return initSqlDataSource.getConnection();
    }

    private boolean shouldFetch() {
        return Duration.between(lastFetchTime, Instant.now()).compareTo(pollInterval) > 0;
    }

    private boolean streamHasData() {
        log.debug("Checking if stream has data");
        try (Connection connection = connection();
                PreparedStatement statement = connection.prepareStatement(STREAM_HAS_DATA_SQL)) {
            statement.setString(1, streamObject.fullName());
            ResultSet results = statement.executeQuery();
            if (results.next()) {
                boolean result = results.getBoolean(1);
                log.debug("Stream has data: {}", result);
                return result;
            }
            log.debug("No results from stream has data");
            return false;
        } catch (SQLException e) {
            log.warn("Failed to check if stream has data", e);
            return false;
        }
    }

    private JdbcCursorItemReader<SnowflakeStreamRow> reader() {
        JdbcCursorItemReaderBuilder<SnowflakeStreamRow> reader = JdbcReaderFactory.create(readerOptions);
        String sql = String.format("SELECT * FROM %s", tempTableName());
        reader.sql(sql);
        reader.name(sql);
        reader.rowMapper(new SnowflakeStreamColumnMapRowMapper());
        reader.dataSource(initSqlDataSource);
        return reader.build();
    }

    private String currentStreamOffset(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(OFFSET_SQL)) {
            statement.setString(1, streamObject.fullName());
            ResultSet results = statement.executeQuery();
            if (results.next()) {
                return results.getString(1);
            }
            throw new IllegalStateException(
                    String.format("Could not retrieve current offset for Snowflake stream '%s'", streamObject));
        } catch (SQLException ex) {
            if (isInvalidStreamName(ex)) {
                // the stream doesn't exist yet
                return null;
            }
            throw ex;
        }
    }

    private boolean isInvalidStreamName(SQLException ex) {
        return StringUtils.hasLength(ex.getMessage()) && ex.getMessage().toLowerCase().contains("must be a valid stream name");
    }

    private String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9]", "_");
    }

    private void fetch() throws Exception {
        try (Connection connection = connection()) {
            // First validate that the source table exists
            validateSourceTable(connection);

            try (PreparedStatement statement = streamInitStatement(connection)) {
                statement.execute();
                log.debug("Initialized stream: {}", streamObject.fullName());
            }
            try (Statement statement = connection.createStatement()) {
                String tempTable = tempTableName();
                String sql = String.format(CREATE_TEMP_TABLE_SQL, tempTable, streamObject.fullName());
                log.debug("Initializing temp table '{}' with statement: {}", tempTable, sql);
                try {
                    statement.execute(sql);
                } catch (SQLException e) {
                    log.error(
                            "Failed to create temp table '{}' from Snowflake stream '{}'. This could be due to: 1) Stream doesn't exist or isn't accessible, 2) Insufficient permissions to create temp table, 3) Source table doesn't exist or isn't accessible",
                            tempTable, streamObject.fullName(), e);
                    throw e;
                }
                log.debug("Initialized temp table: {}", tempTable);
            }
            // now that data has been copied from temp table get the new current offset
            // don't commit it to redis until the current batch is successful
            try {
                nextOffset = currentStreamOffset(connection);
                log.debug("Next offset: '{}'", nextOffset);
            } catch (SQLException e) {
                log.error("Failed to get current stream offset for '{}'", streamObject.fullName(), e);
                throw e;
            }
        }
    }

    private void validateSourceTable(Connection connection) throws SQLException {
        String sql = String.format("SELECT 1 FROM %s LIMIT 1", table);
        try (Statement statement = connection.createStatement()) {
            log.debug("Validating source table exists: {}", table);
            try {
                statement.executeQuery(sql);
            } catch (SQLException e) {
                log.error(
                        "Source table '{}' does not exist or is not accessible. Please verify the table name and your permissions.",
                        table, e);
                throw e;
            }
            log.debug("Source table validated: {}", table);
        }
    }

    private PreparedStatement streamInitStatement(Connection connection) throws Exception {
        Map<String, Object> offsetMap = offsetStore.getOffset();
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
        }
        String offset = (String) offsetMap.get(OFFSET_KEY);
        if (!StringUtils.hasLength(offset) || offset.equals("0")) {
            String sql = createStreamSql(CREATE_STREAM_INIT);
            log.debug("Initializing Snowflake stream: '{}'", sql);
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setBoolean(1, snapshotMode == SnapshotMode.INITIAL);
            return statement;
        }
        String sql = createStreamSql(CREATE_STREAM_OFFSET);
        log.debug("Initializing Snowflake stream with offset {}: '{}'", offset, sql);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, offset);
        return statement;
    }

    private String createStreamSql(String sql) {
        return String.format(sql, streamObject.fullName(), table);
    }

    public String getStreamDatabase() {
        return streamDatabase;
    }

    public void setStreamDatabase(String streamDatabase) {
        this.streamDatabase = streamDatabase;
    }

    public String getStreamSchema() {
        return streamSchema;
    }

    public void setStreamSchema(String streamSchema) {
        this.streamSchema = streamSchema;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public void setSnapshotMode(SnapshotMode snapshotMode) {
        this.snapshotMode = snapshotMode;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public JdbcReaderOptions getReaderOptions() {
        return readerOptions;
    }

    public void setReaderOptions(JdbcReaderOptions readerOptions) {
        this.readerOptions = readerOptions;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

}

