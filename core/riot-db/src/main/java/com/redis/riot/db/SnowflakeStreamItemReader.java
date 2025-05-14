package com.redis.riot.db;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.spring.batch.item.PollableItemReader;
import io.lettuce.core.AbstractRedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SnowflakeStreamItemReader extends AbstractItemCountingItemStreamItemReader<SnowflakeStreamRow>
        implements PollableItemReader<SnowflakeStreamRow> {

    private static final String OFFSET_SQL = "SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP(?)";

    private static final String CREATE_STREAM_SQL = "CREATE OR REPLACE STREAM %s ON TABLE %s";

    private static final String CREATE_TEMP_TABLE_SQL = "CREATE OR REPLACE TABLE %s AS SELECT * FROM %s WHERE METADATA$ACTION <> 'DELETE'";

    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(3);

    public enum SnapshotMode {
        INITIAL, NEVER;
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    private AbstractRedisClient redisClient;

    private JdbcReaderOptions readerOptions = new JdbcReaderOptions();

    private DatabaseObject tableObject;

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
     * Snowflake database object to use for stream and temp table
     */
    private DatabaseObject streamObject;

    private DataSource initSqlDataSource;

    private JdbcCursorItemReader<SnowflakeStreamRow> reader;

    private String nextOffset;

    private long lastFetchTime;

    private Duration pollInterval = DEFAULT_POLL_INTERVAL;

    public SnowflakeStreamItemReader() {
        setName(ClassUtils.getShortName(getClass()));
    }

    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(redisClient, "Redis client is required");
        Assert.notNull(dataSource, "DataSource is required");
        if (initSqlDataSource == null) {
            initSqlDataSource = initSqlDataSource();
        }
        if (streamObject == null) {
            streamObject = new DatabaseObject();
        }
        if (streamObject.getDatabase() == null) {
            streamObject.setDatabase(tableObject.getDatabase());
        }
        if (streamObject.getSchema() == null) {
            streamObject.setSchema(tableObject.getSchema());
        }
        if (streamObject.getTable() == null) {
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

    private String offsetKey() {
        return String.format("riotx:offset:%s", streamObject.fullName());
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
    protected SnowflakeStreamRow doRead() throws Exception {
        throw new UnsupportedOperationException("Read operation is not supported");
    }

    @Override
    public synchronized SnowflakeStreamRow poll(long timeout, TimeUnit unit) throws Exception {
        SnowflakeStreamRow item = reader.read();
        if (item == null) {
            if (nextOffset != null) {
                try (StatefulRedisModulesConnection<String, String> connection = redisConnection()) {
                    connection.sync().set(offsetKey(), nextOffset);
                    log.debug("In snowflake import after success: stored offset {}={}", offsetKey(), nextOffset);
                }
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

    private String currentRedisOffset() {
        try (StatefulRedisModulesConnection<String, String> connection = redisConnection()) {
            return connection.sync().get(offsetKey());
        }
    }

    private StatefulRedisModulesConnection<String, String> redisConnection() {
        return ConnectionBuilder.client(redisClient).connection();
    }

    private String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9]", "_");
    }

    private void fetch() throws SQLException {
        try (Connection connection = initSqlDataSource.getConnection()) {
            String createStreamSQL = String.format(CREATE_STREAM_SQL, streamObject.fullName(), tableObject.fullName());
            String offset = currentRedisOffset();
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

    public DatabaseObject getStreamObject() {
        return streamObject;
    }

    public void setStreamObject(DatabaseObject streamObject) {
        this.streamObject = streamObject;
    }

    public AbstractRedisClient getRedisClient() {
        return redisClient;
    }

    public void setRedisClient(AbstractRedisClient redisClient) {
        this.redisClient = redisClient;
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

    public DatabaseObject getTableObject() {
        return tableObject;
    }

    public void setTableObject(DatabaseObject tableObject) {
        this.tableObject = tableObject;
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

}

