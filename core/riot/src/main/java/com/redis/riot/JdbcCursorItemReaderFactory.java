package com.redis.riot;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.util.Assert;

import com.redis.riot.core.RiotException;

public class JdbcCursorItemReaderFactory {

	public static JdbcCursorItemReaderBuilder<Map<String, Object>> create(DatabaseReaderArgs readerArgs) {
		JdbcCursorItemReaderBuilder<Map<String, Object>> reader = new JdbcCursorItemReaderBuilder<>();
		reader.saveState(false);
		reader.rowMapper(new ColumnMapRowMapper());
		reader.fetchSize(readerArgs.getFetchSize());
		reader.maxRows(readerArgs.getMaxRows());
		reader.queryTimeout(Math.toIntExact(readerArgs.getQueryTimeout().getValue().toMillis()));
		reader.useSharedExtendedConnection(readerArgs.isUseSharedExtendedConnection());
		reader.verifyCursorPosition(readerArgs.isVerifyCursorPosition());
		if (readerArgs.getMaxItemCount() > 0) {
			reader.maxItemCount(readerArgs.getMaxItemCount());
		}
		return reader;
	}

}
