package com.redis.riotx;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.riot.DatabaseImport;
import com.redis.riot.RedisContext;
import com.redis.riot.core.RiotException;

import io.lettuce.core.api.sync.RedisCommands;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(name = "snowflake-import", description = "Import from a snowflake table (uses Snowflake Streams to track changes).")
public class SnowflakeImport extends DatabaseImport {

	private String table;
	private String fullStreamName;
	private String tempTable;
	private String offsetKey;
	private RedisContext redisContext;

	// reuse the sql param
	// TODO find a way to override the description or method on that param or maybe
	// this shouldnt extend DatabaseImport
	// @Option(names = "--table", required = true, description = "Fully qualified
	// table: ex MYDATABASE.MYSCHEMA.TABLE", paramLabel = "<table>")
	// private String table;

	@Option(names = "--start-from-beginning", description = "Create the snowflake stream with SHOW_INITIAL_ROWS=TRUE to import initial data + changes from that point.")
	private boolean fromBeginning;

	public boolean isFromBeginning() {
		return fromBeginning;
	}

	public void setFromBeginning(boolean fromBeginning) {
		this.fromBeginning = fromBeginning;
	}

	private String getCurrentOffset(Connection sqlConnection, String fullStreamName) throws SQLException {
		String offsetSql = MessageFormat.format("SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP(''{0}'')", fullStreamName);

		try {
			ResultSet results = sqlConnection.prepareStatement(offsetSql).executeQuery();
			if (results.next()) {
				return results.getString(1);
			} else {
				throw new RuntimeException(
						"getCurrentOffset query did not return any results. This should not happen: " + offsetSql);
			}
		} catch (SQLException ex) {
			if (ex.getMessage().contains("must be a valid stream name")) {
				// the stream doesn't exist yet
				return null;
			} else {
				throw ex;
			}
		}
	}

	private Runnable afterSuccess(Connection sqlConnection, RedisContext redisContext, String fullStreamName,
			String offsetKey, String tempTable) {
		RedisCommands<String, String> syncCommands = redisContext.getConnection().sync();

		return () -> {
			String newOffset = null;
			try {
				newOffset = getCurrentOffset(sqlConnection, fullStreamName);
				syncCommands.set(offsetKey, newOffset);
				log.debug("in snowflake import after success: stored offset {}={}", offsetKey, newOffset);

				// TODO cleanup but ensure this doesn't interfere with any other tasks or jobs
				// sqlConnection.prepareStatement(MessageFormat.format("DROP TABLE {0}",
				// tempTable)).execute();
			} catch (SQLException e) {
				throw new RuntimeException("Error getting and setting stream offset in afterSuccess callback", e);
			}
		};

	}

	private String initStatement(String currentOffset, String fullStreamName) {
		String initStatement = null;
		if (currentOffset != null) {
			initStatement = MessageFormat.format(
					"CREATE OR REPLACE STREAM {0} ON TABLE {1} AT (TIMESTAMP => TO_TIMESTAMP(''{2}''))", fullStreamName,
					table, currentOffset);
		} else {
			initStatement = MessageFormat.format("CREATE OR REPLACE STREAM {0} ON TABLE {1}", fullStreamName, table);
			if (fromBeginning) {
				initStatement += " SHOW_INITIAL_ROWS=TRUE";
			}
		}
		return initStatement;
	}

	@Override
	protected void initialize() {
		super.initialize();

		this.table = sql;
		String[] tableParts = sql.split("\\.");
		Assert.isTrue(tableParts.length == 3, "Must provide table in format: DATABASE.SCHEMA.TABLE, found: " + sql);

		String database = tableParts[0];
		String schema = tableParts[1];
		String simpleTable = tableParts[2];

		String streamName = simpleTable + "_changestream";
		fullStreamName = MessageFormat.format("{0}.{1}.{2}", database, schema, streamName);
		tempTable = fullStreamName + "_temp";
		offsetKey = MessageFormat.format("riotx:offset:{0}", fullStreamName);

		redisContext = this.targetRedisContext();
		redisContext.afterPropertiesSet();

		// set sql command to consume from temp table
		sql = MessageFormat.format("SELECT * FROM {0}", tempTable);
	}

	@Override
	protected JdbcCursorItemReader<Map<String, Object>> reader() {
		JdbcCursorItemReader<Map<String, Object>> itemReader = super.reader();
		DataSource dataSource;
		try {
			dataSource = getDataSourceArgs().dataSource();
		} catch (Exception e) {
			throw new RiotException("Could not initialize data source", e);
		}
		itemReader.setPreparedStatementSetter(ps -> setValues(dataSource, ps));
		return itemReader;
	}

	private void setValues(DataSource dataSource, PreparedStatement ps) throws SQLException {
		Connection dbConnection = dataSource.getConnection();
		StatefulRedisModulesConnection<String, String> connection = redisContext.getConnection();
		RedisCommands<String, String> syncCommands = connection.sync();
		String currentOffset = syncCommands.get(offsetKey);

		// get current offset of stream and save to target redis
		String newOffset = getCurrentOffset(dbConnection, fullStreamName);
		if (null != newOffset) {
			syncCommands.set(offsetKey, newOffset);
			log.debug("in snowflake reader setup: stored offset {}={}", offsetKey, newOffset);
		}

		String initStatement = initStatement(currentOffset, fullStreamName);

		// TODO figure out why we cant use temp table here - could be multiple
		// different connection/sessions use it
		String initTempTable = MessageFormat.format("CREATE OR REPLACE TABLE {0} AS SELECT * FROM {1}", tempTable,
				fullStreamName);

		// set success callback
		this.onJobSuccessCallback = afterSuccess(dbConnection, redisContext, fullStreamName, offsetKey, tempTable);

		// initialize stream and copy data to temp table
		dbConnection.prepareStatement(initStatement).execute();
		log.debug("initialized stream: {}", initStatement);

		dbConnection.prepareStatement(initTempTable).execute();
		log.debug("initialized temp table: {}", initTempTable);
	}
}
