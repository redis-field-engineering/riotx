package com.redis.riot.db;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.spring.batch.item.PollableItemReader;
import io.lettuce.core.AbstractRedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SnowflakeStreamItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>>
        implements PollableItemReader<Map<String, Object>> {

    private static final String OFFSET_SQL = "SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP(?)";

    private static final String CREATE_STREAM_SQL = "CREATE OR REPLACE STREAM %s ON TABLE %s";

    private static final String CREATE_TEMP_TABLE_SQL = "CREATE OR REPLACE TABLE %s AS SELECT * FROM %s";

    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(5);

    public enum SnapshotMode {
        INITIAL, NEVER;
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    private AbstractRedisClient redisClient;

    private JdbcReaderOptions readerOptions = new JdbcReaderOptions();

    private String tableOrView;

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
     * Snowflake CDC database to use for stream and temp table
     */
    private String cdcDatabase;

    /**
     * Snowflake CDC schema to use for stream and temp table
     */
    private String cdcSchema;

    private DataSource initSqlDataSource;

    private JdbcCursorItemReader<Map<String, Object>> reader;

    private StreamInfo streamInfo;

    private String nextOffset;

    private long lastFetchTime;

    private Duration pollInterval = DEFAULT_POLL_INTERVAL;

    public SnowflakeStreamItemReader() {
        setName(ClassUtils.getShortName(getClass()));
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
        if (reader == null) {
            reader = reader();
            reader.open(executionContext);
        }
    }

    @Override
    protected synchronized void doOpen() throws Exception {
        Assert.notNull(redisClient, "Redis client is required");
        Assert.notNull(dataSource, "DataSource is required");
        if (initSqlDataSource == null) {
            initSqlDataSource = initSqlDataSource();
        }
        if (streamInfo == null) {
            streamInfo = streamInfo();
        }
        if (nextOffset == null) {
            fetch();
        }
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
    public synchronized void close() throws ItemStreamException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        super.close();
    }

    @Override
    protected synchronized void doClose() {
        nextOffset = null;
        streamInfo = null;
        initSqlDataSource = null;
    }

    @Override
    protected Map<String, Object> doRead() throws Exception {
        throw new UnsupportedOperationException("Read operation is not supported");
    }

    @Override
    public synchronized Map<String, Object> poll(long timeout, TimeUnit unit) throws Exception {
        Map<String, Object> item = reader.read();
        if (item == null) {
            if (nextOffset != null) {
                afterSuccess();
                nextOffset = null;
            }
            if (shouldFetch()) {
                fetch();
                reader.close();
                reader = reader();
                reader.open(new ExecutionContext());
                return reader.read();
            }
        }
        return item;
    }

    private boolean shouldFetch() {
        return Duration.ofMillis(System.currentTimeMillis() - lastFetchTime).compareTo(pollInterval) > 0;
    }

    private JdbcCursorItemReader<Map<String, Object>> reader() {
        String sql = selectSQL();
        return JdbcReaderFactory.create(readerOptions).sql(sql).name(sql).dataSource(initSqlDataSource).build();
    }

    private String currentStreamOffset(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(OFFSET_SQL)) {
            statement.setString(1, streamInfo.getFullStreamName());
            ResultSet results = statement.executeQuery();
            if (results.next()) {
                return results.getString(1);
            }
            throw new IllegalStateException(String.format("Could not retrieve current offset for Snowflake stream '%s'",
                    streamInfo.getFullStreamName()));
        } catch (SQLException ex) {
            if (StringUtils.hasLength(ex.getMessage()) && ex.getMessage().toLowerCase()
                    .contains("must be a valid stream name")) {
                // the stream doesn't exist yet
                return null;
            }
            throw ex;
        }
    }

    private void afterSuccess() throws SQLException {
        try (StatefulRedisModulesConnection<String, String> connection = redisConnection()) {
            connection.sync().set(streamInfo.getOffsetKey(), nextOffset);
            log.debug("In snowflake import after success: stored offset {}={}", streamInfo.getOffsetKey(), nextOffset);
        }
        try (Connection sqlConnection = initSqlDataSource.getConnection()) {
            sqlConnection.prepareStatement(String.format("DROP TABLE %s", streamInfo.getTempTable())).execute();
        }
    }

    private String currentRedisOffset() {
        try (StatefulRedisModulesConnection<String, String> connection = redisConnection()) {
            return connection.sync().get(streamInfo.getOffsetKey());
        }
    }

    private StatefulRedisModulesConnection<String, String> redisConnection() {
        return ConnectionBuilder.client(redisClient).connection();
    }

    private StreamInfo streamInfo() {
        String objectRegex = "^(?<database>[a-zA-Z0-9_$]+)\\.(?<schema>[a-zA-Z0-9_$]+)\\.(?<table>[a-zA-Z0-9_$]+)$";
        Pattern objectPattern = Pattern.compile(objectRegex);
        Matcher objectMatcher = objectPattern.matcher(tableOrView);
        Assert.isTrue(objectMatcher.matches(),
                () -> String.format("Must provide table or view in format: DATABASE.SCHEMA.TABLE, found %s", tableOrView));
        String database = objectMatcher.group("database");
        String schema = objectMatcher.group("schema");
        String simpleTable = objectMatcher.group("table");

        String useDatabase = cdcDatabase == null ? database : cdcDatabase;
        String useSchema = cdcSchema == null ? schema : cdcSchema;

        String streamName = String.format("%s_changestream", simpleTable);
        String fullStreamName = String.format("%s.%s.%s", useDatabase, useSchema, streamName);
        String tempTable = String.format("%s_temp", fullStreamName);
        String offsetKey = String.format("riotx:offset:%s", fullStreamName);
        StreamInfo streamInfo = new StreamInfo();
        streamInfo.setOffsetKey(offsetKey);
        streamInfo.setTempTable(tempTable);
        streamInfo.setFullStreamName(fullStreamName);
        return streamInfo;
    }

    private String selectSQL() {
        return String.format("SELECT * FROM %s", streamInfo.getTempTable());
    }

    private String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9]", "_");
    }

    private void fetch() throws SQLException {
        try (Connection connection = initSqlDataSource.getConnection()) {
            String offset = currentRedisOffset();
            if (!StringUtils.hasLength(offset) || offset.equals("0")) {
                String initialSQL = initialSQL();
                try (Statement statement = connection.createStatement()) {
                    statement.execute(initialSQL);
                }
                log.debug("Initialized Snowflake stream: '{}'", initialSQL);
            } else {
                String offsetSQL = streamSQL() + " AT (TIMESTAMP => TO_TIMESTAMP(?))";
                try (PreparedStatement statement = connection.prepareStatement(offsetSQL)) {
                    statement.setString(1, offset);
                    statement.execute();
                }
                log.debug("Initialized Snowflake stream with offset {}: '{}'", offset, offsetSQL);
            }
            String tempTableSQL = tempTableSQL();
            try (Statement statement = connection.createStatement()) {
                statement.execute(tempTableSQL);
            }
            log.debug("Initialized temp table: '{}'", tempTableSQL);
            // now that data has been copied from temp table get the new current offset
            // don't commit it to redis until the current batch is successful

            nextOffset = currentStreamOffset(connection);
            lastFetchTime = System.currentTimeMillis();
        }
    }

    private String initialSQL() {
        String sql = streamSQL();
        if (snapshotMode == SnapshotMode.INITIAL) {
            return sql + " SHOW_INITIAL_ROWS=TRUE";
        }
        return sql;
    }

    private String tempTableSQL() {
        return String.format(CREATE_TEMP_TABLE_SQL, streamInfo.getTempTable(), streamInfo.getFullStreamName());
    }

    private String streamSQL() {
        return String.format(CREATE_STREAM_SQL, streamInfo.getFullStreamName(), tableOrView);
    }

    private static class StreamInfo {

        private String fullStreamName;

        private String tempTable;

        private String offsetKey;

        public String getFullStreamName() {
            return fullStreamName;
        }

        public void setFullStreamName(String fullStreamName) {
            this.fullStreamName = fullStreamName;
        }

        public String getTempTable() {
            return tempTable;
        }

        public void setTempTable(String tempTable) {
            this.tempTable = tempTable;
        }

        public String getOffsetKey() {
            return offsetKey;
        }

        public void setOffsetKey(String offsetKey) {
            this.offsetKey = offsetKey;
        }

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

    public String getTableOrView() {
        return tableOrView;
    }

    public void setTableOrView(String tableOrView) {
        this.tableOrView = tableOrView;
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

