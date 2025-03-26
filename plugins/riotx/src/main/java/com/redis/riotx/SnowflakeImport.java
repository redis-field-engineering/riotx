package com.redis.riotx;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
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

	@Parameters(arity = "1", description = "Fully qualified Snowflake Table or View, eg: DB.SCHEMA.TABLE", paramLabel = "TABLE")
	private String tableOrView;

	@ArgGroup(exclusive = false)
	private DatabaseReaderArgs readerArgs = new DatabaseReaderArgs();

	private String cdcObject;
	private String fullStreamName;
	private String tempTable;
	private String offsetKey;
	private RedisContext redisContext;
	private String sql;
	private String createViewStatement;

	@Option(names = "--snapshot-mode", description = "Snapshot mode: ${COMPLETION-CANDIDATES} . INITIAL is the default and will set the Snowflake Stream SHOW_INITIAL_ROWS=TRUE option",
			defaultValue = "INITIAL")
	private SnapshotMode snapshotMode;

	@Option(names = "--stream-select-statement", description = "A SQL statement to define a subset of the data to be imported and tracked. This will define a view in Snowflake. A stream will then be created ontop of this view.")
	private String streamSql;

	@Option(names = "--temp-table-database", description = "The database to create the temp table that holds a snapshot of the stream in. By default it will be created in the same database as the source object")
	private String tempTableDatabase;

	@Option(names = "--temp-table-schema", description = "The schema to create the temp table that holds a snapshot of the stream in. By default it will be created in the same schema as the source object")
	private String tempTableSchema;

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
						"Could not retrieve current offset of Snowflake stream: %s".formatted(fullStreamName));
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

	private String getObjectType(Connection sqlConnection, String fullyQualifiedObject) {
		if (streamSql != null) {
			return "VIEW";
		}

		String[] objectParts = fullyQualifiedObject.split("\\.");
		String database = objectParts[0].toUpperCase();
		String schema = objectParts[1].toUpperCase();
		String objectName = objectParts[2].toUpperCase();

		String sql = """
                SELECT TABLE_TYPE
                FROM %s.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """.formatted(database);

		try (PreparedStatement stmt = sqlConnection.prepareStatement(sql)) {
			stmt.setString(1, schema);
			stmt.setString(2, objectName);

			log.info("getObjectType SQL: {}, schema: {}, objectName: {}", stmt, schema, objectName);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					String tableType = rs.getString("TABLE_TYPE");
					if ("BASE TABLE".equals(tableType)) {
						return "TABLE";
					} else if ("VIEW".equals(tableType)) {
						return "VIEW";
					} else {
						throw new RiotException("Only tables and views are supported. %s.%s.%s is a %s".formatted(
								database,
								schema,
								objectName,
								tableType));
					}
				} else {
					throw new RiotException("Was unable to determine the Snowflake object type for %s.%s.%s, check that it exists"
							.formatted(database, schema, objectName));
				}
			}
		} catch (Exception e) {
			throw new RiotException("Was unable to determine the Snowflake object type for %s.%s.%s"
					.formatted(database, schema, objectName), e);
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

	private String initStatement(String currentOffset, String fullStreamName, String objectType) {
		String initStatement = null;
		if (currentOffset != null && !currentOffset.equals("0")) {
			initStatement ="CREATE OR REPLACE STREAM %s ON %s %s AT (TIMESTAMP => TO_TIMESTAMP(?))".formatted(
					fullStreamName,
					objectType,
					cdcObject);
		} else {
			initStatement = "CREATE OR REPLACE STREAM %s ON %s %s".formatted(
					fullStreamName,
					objectType,
					cdcObject);

			if (snapshotMode == SnapshotMode.INITIAL) {
				initStatement += " SHOW_INITIAL_ROWS=TRUE";
			}
		}
		return initStatement;
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

			String streamName = "%s_changestream".formatted(simpleTable);
			fullStreamName ="%s.%s.%s".formatted(database, schema, streamName);
			tempTable =  "%s_temp".formatted(fullStreamName);

			offsetKey = MessageFormat.format("riotx:offset:{0}", fullStreamName);

			redisContext = this.targetRedisContext();
			redisContext.afterPropertiesSet();

			if (streamSql != null){
				String viewName = "%s.%s.%s_view".formatted(database, schema, simpleTable);
				createViewStatement = "CREATE OR REPLACE VIEW %s AS %s".formatted(viewName, streamSql);
				sql = "SELECT * FROM %s".formatted(viewName);
				cdcObject = viewName;
			} else {
				sql = "SELECT * FROM %s".formatted(tempTable);
				cdcObject = tableOrView;
			}
		} else {
			throw new RiotException("Must provide table or view in format: DATABASE.SCHEMA.TABLE, found %s".formatted(tableOrView));
		}
	}

	@Override
	protected Job job() {
		return job(step(reader()));
	}

	protected JdbcCursorItemReader<Map<String, Object>> reader() {
		DataSource dataSource;
		try {
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

			// get current offset of stream and save to target redis
			String objectType = getObjectType(dbConnection, cdcObject);
			if (createViewStatement != null){
				// see if we need to create the view, should only do this once
				if (objectType == null){
					dbConnection.prepareStatement(createViewStatement).execute();
				}
			}

			String initStatement = initStatement(currentOffset, fullStreamName, objectType);

			// TODO figure out why we cant use a temp table here - could be multiple different connection/sessions use it
			String initTempTable = "CREATE OR REPLACE TABLE %s AS SELECT * FROM %s".formatted(tempTable, fullStreamName);

			// initialize stream and copy data to temp table
			log.info("initStatement: {}", initStatement);

			PreparedStatement preparedInitStatement = dbConnection.prepareStatement(initStatement);
			if (initStatement.contains("?")) {
				preparedInitStatement.setString(1, currentOffset);
			}

			preparedInitStatement.execute();
			log.debug("initialized stream: {}", initStatement);

			dbConnection.prepareStatement(initTempTable).execute();
			log.debug("initialized temp table: {}", initTempTable);

			// now that data has been copied from temp table get the new current offset
			// don't commit it to redis until the job is successful
			String newOffset = getCurrentOffset(dbConnection, fullStreamName);
			onJobSuccessCallback = afterSuccess(dbConnection, redisContext, fullStreamName, offsetKey, tempTable, newOffset);
		}
	}
}
