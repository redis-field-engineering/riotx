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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SnowflakeStreamItemReader extends AbstractCountingPollableItemReader<SnowflakeStreamRow> {

    private static final String OFFSET_SQL = "SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP(?)";

    private static final String STREAM_HAS_DATA_SQL = "SELECT SYSTEM$STREAM_HAS_DATA(?)";

    private static final String CREATE_STREAM_SQL = "CREATE OR REPLACE STREAM %s ON TABLE %s";

    private static final String CREATE_TEMP_TABLE_SQL = "CREATE OR REPLACE TABLE %s AS SELECT * FROM %s WHERE METADATA$ACTION <> 'DELETE'";

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

    private long lastFetchTime;

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
            streamObject.setTable(String.format("%s_changestream", tableObject.getTable()));
        }
        if (nextOffset == null) {
            fetch();
        }
        if (reader == null) {
            reader = reader();
            reader.open(new ExecutionContext());
        }
    }

    private String tempTable() {
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
                try (Connection sqlConnection = initSqlDataSource.getConnection()) {
                    sqlConnection.prepareStatement(String.format("DROP TABLE %s", tempTable())).execute();
                }
                nextOffset = null;
            }
            if (shouldFetch()) {
                reader.close();
                fetch();
                reader.open(new ExecutionContext());
                item = reader.read();
            }
        }
        return item;
    }

    private boolean shouldFetch() {
        return Duration.ofMillis(System.currentTimeMillis() - lastFetchTime).compareTo(pollInterval) > 0;
    }

    private JdbcCursorItemReader<SnowflakeStreamRow> reader() {
        JdbcCursorItemReaderBuilder<SnowflakeStreamRow> reader = JdbcReaderFactory.create(readerOptions);
        String sql = String.format("SELECT * FROM %s", tempTable());
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
            if (StringUtils.hasLength(ex.getMessage()) && ex.getMessage().toLowerCase()
                    .contains("must be a valid stream name")) {
                // the stream doesn't exist yet
                return null;
            }
            throw ex;
        }
    }

    private boolean streamHasData(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(STREAM_HAS_DATA_SQL)) {
            statement.setString(1, streamObject.fullName());
            ResultSet results = statement.executeQuery();
            if (results.next()) {
                return results.getBoolean(1);
            }
            return false;
        } catch (SQLException ex) {
            if (StringUtils.hasLength(ex.getMessage()) && ex.getMessage().toLowerCase()
                    .contains("must be a valid stream name")) {
                // the stream doesn't exist yet, assume no data
                return false;
            }
            throw ex;
        }
    }

    private String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9]", "_");
    }

    private void fetch() throws Exception {
        try (Connection connection = initSqlDataSource.getConnection()) {
            String createStreamSQL = String.format(CREATE_STREAM_SQL, streamObject.fullName(), table);
            Map<String, Object> offsetMap = offsetStore.getOffset();
            if (offsetMap == null) {
                offsetMap = new HashMap<>();
            }
            String offset = (String) offsetMap.get(OFFSET_KEY);
            if (!StringUtils.hasLength(offset) || offset.equals("0")) {
                String initialSQL = createStreamSQL;
                if (snapshotMode == SnapshotMode.INITIAL) {
                    initialSQL += " SHOW_INITIAL_ROWS=TRUE";
                }
                try (Statement statement = connection.createStatement()) {
                    statement.execute(initialSQL);
                }
                log.debug("Initialized Snowflake stream: '{}'", initialSQL);
            } else {
                String offsetSQL = createStreamSQL + " AT (TIMESTAMP => TO_TIMESTAMP(?))";
                try (PreparedStatement statement = connection.prepareStatement(offsetSQL)) {
                    statement.setString(1, offset);
                    statement.execute();
                }
                log.debug("Initialized Snowflake stream with offset {}: '{}'", offset, offsetSQL);
            }
            
            // Check if stream has data before creating expensive temporary table
            if (!streamHasData(connection)) {
                log.debug("Stream '{}' has no data, skipping temporary table creation", streamObject.fullName());
                nextOffset = currentStreamOffset(connection);
                lastFetchTime = System.currentTimeMillis();
                return;
            }
            
            String tempTableSQL = String.format(CREATE_TEMP_TABLE_SQL, tempTable(), streamObject.fullName());
            try (Statement statement = connection.createStatement()) {
                statement.execute(tempTableSQL);
            }
            log.debug("Initialized temp table: '{}'", tempTableSQL);
            // now that data has been copied from temp table get the new current offset
            // don't commit it to redis until the current batch is successful

            nextOffset = currentStreamOffset(connection);
            log.debug("Next offset: '{}'", nextOffset);
        }
        lastFetchTime = System.currentTimeMillis();
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

