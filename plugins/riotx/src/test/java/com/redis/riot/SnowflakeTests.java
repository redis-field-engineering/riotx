package com.redis.riot;

import com.redis.riot.db.DataSourceBuilder;
import com.redis.riot.db.SnowflakeStreamItemReader;
import com.redis.riot.db.SnowflakeStreamRow;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.slf4j.event.Level;
import org.slf4j.simple.SimpleLogger;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.support.ListItemWriter;
import picocli.CommandLine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;

@EnabledIfEnvironmentVariable(named = "JDBC_URL", matches = ".+")
class SnowflakeTests extends AbstractRiotApplicationTestBase {

    static {
        System.setProperty(SimpleLogger.LOG_KEY_PREFIX + "com.redis.riot.db", Level.DEBUG.name());
    }

    private static final RedisContainer redis = new RedisContainer(
            RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));

    private static final String ENV_URL = "JDBC_URL";

    private static final String ENV_USERNAME = "JDBC_USERNAME";

    private static final String ENV_PASSWORD = "JDBC_PASSWORD";

    protected Connection dbConnection;

    protected DataSource dataSource;

    private SqlScriptRunner sqlRunner;

    private DataSourceBuilder getUnitTestDbDataSourceBuilder() {
        DataSourceBuilder builder = new DataSourceBuilder();
        builder.url(System.getenv(ENV_URL));
        builder.username(System.getenv(ENV_USERNAME));
        builder.password(System.getenv(ENV_PASSWORD));
        builder.driver(SnowflakeImport.SNOWFLAKE_DRIVER);
        builder.minimumIdle(1);
        builder.maximumPoolSize(3);
        return builder;
    }

    @BeforeAll
    void setupLogging() {
        // Remove existing handlers
        SLF4JBridgeHandler.removeHandlersForRootLogger();

        // Install SLF4J bridge
        SLF4JBridgeHandler.install();
    }

    @BeforeAll
    public void setupConnection() throws Exception {
        dataSource = getUnitTestDbDataSourceBuilder().build();
        dbConnection = dataSource.getConnection();
        sqlRunner = new SqlScriptRunner(dbConnection, Map.of("{{PASSWORD}}", System.getenv(ENV_PASSWORD)));
    }

    @AfterAll
    public void teardownContainers() throws SQLException {
        if (dbConnection != null) {
            dbConnection.close();
        }
    }

    @Override
    protected RedisServer getTargetRedisServer() {
        return redis;
    }

    @Override
    protected RedisServer getRedisServer() {
        return redis;
    }

    @Test
    void testReader(TestInfo info) throws Exception {
        sqlRunner.executeScript("db/snowflake-roles.sql");
        sqlRunner.executeScript("db/snowflake-setup-data.sql");
        ListItemWriter<SnowflakeStreamRow> writer = new ListItemWriter<>();
        String name = name(info);
        FlushingStepBuilder<SnowflakeStreamRow, SnowflakeStreamRow> step = new FlushingStepBuilder<>(
                step(name, DEFAULT_CHUNK_SIZE));
        step.reader(reader());
        step.writer(writer);
        step.idleTimeout(Duration.ofSeconds(10));
        step.listener(new ItemWriteListener<>() {

            private boolean executed;

            @Override
            public synchronized void afterWrite(Chunk<? extends SnowflakeStreamRow> items) {
                if (!executed) {
                    try {
                        sqlRunner.executeScript("db/snowflake-insert-more-data.sql");
                        executed = true;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
        run(job(info).start(step.build()).build());
        awaitUntil(() -> size(writer) == 1100);
        log.info("Waiting for more items to be processed");
    }

    private <T> int size(ListItemWriter<T> writer) {
        int size = writer.getWrittenItems().size();
        log.info("Processed {} items", size);
        return size;
    }

    private SnowflakeStreamItemReader reader() {
        SnowflakeStreamItemReader reader = new SnowflakeStreamItemReader();
        reader.setDataSource(dataSource);
        reader.setStreamSchema("raw_pos_cdc");
        reader.setPollInterval(Duration.ofSeconds(1));
        reader.setRole("riotx_cdc");
        reader.setWarehouse("compute_wh");
        reader.setSnapshotMode(SnowflakeStreamItemReader.SnapshotMode.INITIAL);
        reader.setTable("tb_101.raw_pos.incremental_order_header");
        return reader;
    }

    @Test
    void rdiStreamImport(TestInfo info) throws Exception {
        sqlRunner.executeScript("db/snowflake-roles.sql");
        sqlRunner.executeScript("db/snowflake-setup-data.sql");
        execute(info, "snowflake-import-rdi", this::executeSnowflakeImport);
        List<StreamMessage<String, String>> messages = redisCommands.xrange("rdi:tb_101.raw_pos.incremental_order_header",
                Range.create("-", "+"));
        Assertions.assertEquals(100, messages.size());
    }

    @Test
    void hashImport(TestInfo info) throws Exception {
        sqlRunner.executeScript("db/snowflake-roles.sql");
        sqlRunner.executeScript("db/snowflake-setup-data.sql");

        execute(info, "snowflake-import", this::executeSnowflakeImport);
        try (Statement statement = dbConnection.createStatement()) {
            statement.execute("SELECT COUNT(*) AS count FROM tb_101.raw_pos.incremental_order_header");
            ResultSet resultSet = statement.getResultSet();
            resultSet.next();
            Assertions.assertEquals(resultSet.getLong(1), keyCount("orderheader:*"));
            Map<String, String> order = redisCommands.hgetall("orderheader:4063758");
            Assertions.assertEquals("16", order.get("TRUCK_ID"));
            Assertions.assertEquals("21202", order.get("SHIFT_ID"));
        }

        //        sqlRunner.executeScript("db/snowflake-insert-more-data.sql");
        //        execute(info, "snowflake-import", this::executeSnowflakeImport);
        //
        //        try (Statement statement = dbConnection.createStatement()) {
        //            statement.execute("SELECT COUNT(*) AS count FROM tb_101.raw_pos.incremental_order_header");
        //            ResultSet resultSet = statement.getResultSet();
        //            resultSet.next();
        //
        //            long count = resultSet.getLong(1);
        //
        //            Assertions.assertEquals(1100, count);
        //            Assertions.assertEquals(count, keyCount("orderheader:*"));
        //
        //            Map<String, String> order = redisCommands.hgetall("orderheader:4064758");
        //            Assertions.assertEquals("18", order.get("TRUCK_ID"));
        //            Assertions.assertEquals("21207", order.get("SHIFT_ID"));
        //        }
    }

    protected int executeSnowflakeImport(CommandLine.ParseResult parseResult) {
        SnowflakeImport command = command(parseResult);
        command.getFlushingStepArgs().setIdleTimeout(Duration.ofSeconds(10));
        configureDatabase(command.getDataSourceArgs());
        return CommandLine.ExitCode.OK;
    }

    private void configureDatabase(DataSourceArgs args) {
        DataSourceBuilder builder = getUnitTestDbDataSourceBuilder();
        args.setUrl(builder.url());
        args.setUsername(builder.username());
        args.setPassword(builder.password());
        args.setMaxPoolSize(builder.maximumPoolSize());
        args.setMinIdle(builder.minimumIdle());
    }

}
