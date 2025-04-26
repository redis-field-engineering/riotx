package com.redis.riot;

import com.redis.riot.db.JdbcReaderOptions;
import org.springframework.batch.item.database.AbstractCursorItemReader;

import lombok.ToString;
import picocli.CommandLine.Option;

import java.time.Duration;

@ToString
public class DatabaseReaderArgs {

    @Option(names = "--max", description = "Max number of rows to import.", paramLabel = "<count>")
    private int maxItemCount;

    @Option(names = "--fetch", description = "Number of rows to return with each fetch.", paramLabel = "<size>")
    private int fetchSize = JdbcReaderOptions.DEFAULT_FETCH_SIZE;

    @Option(names = "--rows", description = "Max number of rows the ResultSet can contain.", paramLabel = "<count>")
    private int maxRows = JdbcReaderOptions.DEFAULT_MAX_RESULT_SET_ROWS;

    @Option(names = "--query-timeout", description = "The duration for the query to timeout.", paramLabel = "<dur>")
    private Duration queryTimeout;

    @Option(names = "--shared-connection", description = "Use same connection for cursor and other processing.", hidden = true)
    private boolean useSharedExtendedConnection;

    @Option(names = "--verify", description = "Verify position of result set after row mapper.", hidden = true)
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

    public JdbcReaderOptions readerOptions() {
        JdbcReaderOptions options = new JdbcReaderOptions();
        options.setFetchSize(fetchSize);
        options.setMaxRows(maxRows);
        options.setMaxItemCount(maxItemCount);
        options.setUseSharedExtendedConnection(useSharedExtendedConnection);
        options.setQueryTimeout(queryTimeout);
        options.setVerifyCursorPosition(verifyCursorPosition);
        return options;
    }

}
