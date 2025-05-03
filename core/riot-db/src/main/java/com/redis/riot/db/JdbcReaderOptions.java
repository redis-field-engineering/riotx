package com.redis.riot.db;

import org.springframework.batch.item.database.AbstractCursorItemReader;

import lombok.ToString;

import java.time.Duration;

@ToString
public class JdbcReaderOptions {

    public static final int DEFAULT_FETCH_SIZE = AbstractCursorItemReader.VALUE_NOT_SET;

    public static final int DEFAULT_MAX_RESULT_SET_ROWS = AbstractCursorItemReader.VALUE_NOT_SET;

    private int maxItemCount;

    private int fetchSize = DEFAULT_FETCH_SIZE;

    private int maxRows = DEFAULT_MAX_RESULT_SET_ROWS;

    private Duration queryTimeout;

    private boolean useSharedExtendedConnection;

    private boolean verifyCursorPosition;

    public int getMaxItemCount() {
        return maxItemCount;
    }

    public void setMaxItemCount(int maxItemCount) {
        this.maxItemCount = maxItemCount;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    /**
     * The max number of rows the {@link java.sql.ResultSet} can contain
     *
     * @return
     */
    public int getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(int maxResultSetRows) {
        this.maxRows = maxResultSetRows;
    }

    public Duration getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(Duration queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public boolean isUseSharedExtendedConnection() {
        return useSharedExtendedConnection;
    }

    public void setUseSharedExtendedConnection(boolean useSharedExtendedConnection) {
        this.useSharedExtendedConnection = useSharedExtendedConnection;
    }

    public boolean isVerifyCursorPosition() {
        return verifyCursorPosition;
    }

    public void setVerifyCursorPosition(boolean verifyCursorPosition) {
        this.verifyCursorPosition = verifyCursorPosition;
    }

}
