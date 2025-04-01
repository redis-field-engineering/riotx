package com.redis.riotx;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import com.redis.riot.DataSourceArgs;
import com.redis.riot.DatabaseReaderArgs;
import com.redis.riot.JdbcCursorItemReaderFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.database.JdbcCursorItemReader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.riot.RedisContext;
import com.redis.riot.core.RiotException;

import io.lettuce.core.api.sync.RedisCommands;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@CommandLine.Command(name = "snowflake-import", description = "Import from a snowflake table (uses Snowflake Streams to track changes).")
public class SnowflakeImport extends AbstractTargetRedisImportCommand {
	public enum SnapshotMode {
		INITIAL,
		NEVER
	}

	@ArgGroup(exclusive = false)
	private DataSourceArgs dataSourceArgs = new DataSourceArgs();

	@Parameters(arity = "1", description = "Fully qualified Snowflake Table or Materialized View, eg: DB.SCHEMA.TABLE", paramLabel = "TABLE")
	private String tableOrView;

	@ArgGroup(exclusive = false)
	private DatabaseReaderArgs readerArgs = new DatabaseReaderArgs();

	public final static String SNOWFLAKE_DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver";

	private String cdcObject;
	private String fullStreamName;
	private String tempTable;
	private String offsetKey;
	private RedisContext redisContext;
	private String sql;
	private String createViewStatement;
	private String viewName;

	@Option(names = "--snapshot-mode", description = "Snapshot mode: ${COMPLETION-CANDIDATES} . INITIAL is the default and will set the Snowflake Stream SHOW_INITIAL_ROWS=TRUE option",
			defaultValue = "INITIAL")
	private SnapshotMode snapshotMode;

	private String getCurrentOffset(Connection sqlConnection, String fullStreamName) throws SQLException {
		String offsetSql = "SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP(?)";

		try {
			PreparedStatement stmt = sqlConnection.prepareStatement(offsetSql);
			stmt.setString(1, fullStreamName);
			ResultSet results = stmt.executeQuery();
			if (results.next()) {
				return results.getString(1);
			} else {
				throw new RiotException(
						String.format("Could not retrieve current offset of Snowflake stream: %s", fullStreamName));
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
			String offsetKey, String tempTable, String newOffset) {
		RedisCommands<String, String> syncCommands = redisContext.getConnection().sync();

		return () -> {
			syncCommands.set(offsetKey, newOffset);
			log.debug("in snowflake import after success: stored offset {}={}", offsetKey, newOffset);

			// TODO cleanup but ensure this doesn't interfere with any other tasks or jobs
			// sqlConnection.prepareStatement(MessageFormat.format("DROP TABLE {0}",
			// tempTable)).execute();
		};
	}

	private PreparedStatement initStatement(Connection connection, String currentOffset, String fullStreamName) throws SQLException {
		String initStatement = null;
		boolean hasArg = false;

		if (currentOffset != null && !currentOffset.equals("0")) {
			initStatement = String.format("CREATE OR REPLACE STREAM %s ON TABLE %s AT (TIMESTAMP => TO_TIMESTAMP(?))",
					fullStreamName,
					cdcObject);
			hasArg = true;
		} else {
			initStatement = String.format("CREATE OR REPLACE STREAM %s ON TABLE %s",
					fullStreamName,
					cdcObject);

			if (snapshotMode == SnapshotMode.INITIAL) {
				initStatement += " SHOW_INITIAL_ROWS=TRUE";
			}
		}

		PreparedStatement preparedStatement = connection.prepareStatement(initStatement);
		if (hasArg){
			preparedStatement.setString(1, currentOffset);
		}

		log.debug("initStatement: {}", initStatement);

		return preparedStatement;
	}

	@Override
	protected void initialize() {
		super.initialize();

		String objectRegex = "^(?<database>[a-zA-Z0-9_$]+)\\." +
		                      "(?<schema>[a-zA-Z0-9_$]+)\\." +
				              "(?<table>[a-zA-Z0-9_$]+)$";
		Pattern objectPattern = Pattern.compile(objectRegex);
		Matcher objectMatcher = objectPattern.matcher(tableOrView);

		if (objectMatcher.matches()){
			String database = objectMatcher.group("database");
			String schema = objectMatcher.group("schema");
			String simpleTable = objectMatcher.group("table");

			String streamName = String.format("%s_changestream", simpleTable);
			fullStreamName = String.format("%s.%s.%s", database, schema, streamName);
			tempTable =  String.format("%s_temp", fullStreamName);

			offsetKey = String.format("riotx:offset:%s", fullStreamName);

			redisContext = this.targetRedisContext();
			redisContext.afterPropertiesSet();

			cdcObject = tableOrView;
			sql = String.format("SELECT * FROM %s", tempTable);
		} else {
			throw new RiotException(String.format("Must provide table or view in format: DATABASE.SCHEMA.TABLE, found %s", tableOrView));
		}
	}

	@Override
	protected Job job() {
		return job(step(reader()));
	}

	protected JdbcCursorItemReader<Map<String, Object>> reader() {
		DataSource dataSource;
		try {
			dataSourceArgs.setDriver(SNOWFLAKE_DRIVER);
			dataSource = dataSourceArgs.dataSource();
		} catch (Exception e) {
			throw new RiotException("Could not initialize data source", e);
		}

		JdbcCursorItemReader<Map<String, Object>> itemReader = JdbcCursorItemReaderFactory.createReader(sql,
				dataSourceArgs,
				readerArgs);
		itemReader.setPreparedStatementSetter(ps -> setValues(dataSource, ps));
		return itemReader;
	}

	private void setValues(DataSource dataSource, PreparedStatement ps) throws SQLException {
		try(Connection dbConnection = dataSource.getConnection()){
			StatefulRedisModulesConnection<String, String> connection = redisContext.getConnection();
			RedisCommands<String, String> syncCommands = connection.sync();
			String currentOffset = syncCommands.get(offsetKey);

			// TODO figure out why we cant use a temp table here - could be multiple different connection/sessions use it
			String initTempTable = String.format("CREATE OR REPLACE TABLE %s AS SELECT * FROM %s", tempTable, fullStreamName);

			// initialize stream and copy data to temp table
			PreparedStatement preparedInitStatement = initStatement(dbConnection, currentOffset, fullStreamName);

			preparedInitStatement.execute();

			dbConnection.prepareStatement(initTempTable).execute();
			log.debug("initialized temp table: {}", initTempTable);

			// now that data has been copied from temp table get the new current offset
			// don't commit it to redis until the job is successful
			String newOffset = getCurrentOffset(dbConnection, fullStreamName);
			onJobSuccessCallback = afterSuccess(dbConnection, redisContext, fullStreamName, offsetKey, tempTable, newOffset);
		}
	}
}
