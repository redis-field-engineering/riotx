package com.redis.riot.db;

import java.util.Map;

import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;

public class JdbcReaderFactory {

    public static JdbcCursorItemReaderBuilder<Map<String, Object>> create(JdbcReaderOptions options) {
        JdbcCursorItemReaderBuilder<Map<String, Object>> reader = new JdbcCursorItemReaderBuilder<>();
        reader.saveState(false);
        reader.rowMapper(new VariantColumnMapRowMapper());
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
