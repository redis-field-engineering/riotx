package com.redis.riot.db;

import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;

public class JdbcReaderFactory {

    public static <T> JdbcCursorItemReaderBuilder<T> create(JdbcReaderOptions options) {
        JdbcCursorItemReaderBuilder<T> reader = new JdbcCursorItemReaderBuilder<>();
        reader.saveState(false);
        reader.fetchSize(options.getFetchSize());
        reader.maxRows(options.getMaxRows());
        if (options.getQueryTimeout() != null) {
            reader.queryTimeout(Math.toIntExact(options.getQueryTimeout().toMillis()));
        }
        reader.useSharedExtendedConnection(options.isUseSharedExtendedConnection());
        reader.verifyCursorPosition(options.isVerifyCursorPosition());
        if (options.getMaxItemCount() > 0) {
            reader.maxItemCount(options.getMaxItemCount());
        }
        return reader;
    }

}
