package com.redis.riot;

import com.redis.riot.db.DataSourceBuilder;
import com.redis.riot.db.SnowflakeStreamItemReader;
import com.redis.riot.db.SnowflakeStreamRow;
import com.redis.riot.rdi.ChangeEventToStreamMessage;
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
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemWriter;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@EnabledIfEnvironmentVariable(named = "RIOT_JDBC_URL", matches = ".+")
class SnowflakeTests extends AbstractRiotApplicationTestBase {

    static {
        System.setProperty(SimpleLogger.LOG_KEY_PREFIX + "com.redis.riot.db", Level.DEBUG.name());
    }

    private static final RedisContainer redis = new RedisContainer(
            RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));

    private static final String ENV_URL = "RIOT_JDBC_URL";

    private static final String ENV_USERNAME = "RIOT_JDBC_USER";

    private static final String ENV_PASSWORD = "RIOT_JDBC_PASS";

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
        builder.maximumPoolSize(10);
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
        String streamKey = "data:riotx.raw_pos.incremental_order_header";
        List<StreamMessage<String, String>> messages = redisCommands.xrange(streamKey, Range.create("-", "+"));
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
    }

    protected int executeSnowflakeImport(CommandLine.ParseResult parseResult) {
        SnowflakeImport command = command(parseResult);
        command.getFlushingStepArgs().setIdleTimeout(Duration.ofSeconds(20));
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

    @Test
    void testMultiTableImport(TestInfo info) throws Exception {
        // Create 2 snowflake tables with sample data
        sqlRunner.executeScript("db/snowflake-roles.sql");
        sqlRunner.executeScript("db/snowflake-setup-multi-tables.sql");
        
        // Execute snowflake-import-rdi-multi command
        execute(info, "snowflake-import-rdi-multi", this::executeSnowflakeImport);
        
        // Verify table1 data was imported to Redis streams
        String streamKey1 = "data:riotx.raw_pos.table1";
        List<StreamMessage<String, String>> messages1 = redisCommands.xrange(streamKey1, Range.create("-", "+"));
        Assertions.assertEquals(5, messages1.size());
        
        // Verify table2 data was imported to Redis streams
        String streamKey2 = "data:riotx.raw_pos.table2";
        List<StreamMessage<String, String>> messages2 = redisCommands.xrange(streamKey2, Range.create("-", "+"));
        Assertions.assertEquals(5, messages2.size());
        
        // Verify sample data content from table1
        StreamMessage<String, String> productMessage = messages1.get(0);
        Map<String, String> productData = productMessage.getBody();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode productNode = mapper.readTree(productData.get(ChangeEventToStreamMessage.VALUE));
        JsonNode productValueNode = productNode.get("after");
        Assertions.assertTrue(productValueNode.has("PRODUCT_ID"));
        Assertions.assertTrue(productValueNode.has("PRODUCT_NAME"));
        Assertions.assertTrue(productValueNode.has("CATEGORY"));
        
        // Verify sample data content from table2
        StreamMessage<String, String> customerMessage = messages2.get(0);
        Map<String, String> customerData = customerMessage.getBody();
        JsonNode customerNode = mapper.readTree(customerData.get(ChangeEventToStreamMessage.VALUE));
        JsonNode customerValueNode = customerNode.get("after");
        Assertions.assertTrue(customerValueNode.has("CUSTOMER_ID"));
        Assertions.assertTrue(customerValueNode.has("CUSTOMER_NAME"));
        Assertions.assertTrue(customerValueNode.has("EMAIL"));
    }

    @Test
    void testStreamOperationMappings() throws Exception {
        sqlRunner.executeScript("db/snowflake-roles.sql");
        sqlRunner.executeScript("db/snowflake-setup-data.sql");

        SnowflakeStreamItemReader reader = reader();
        reader.setSnapshotMode(SnowflakeStreamItemReader.SnapshotMode.NEVER);
        reader.open(new ExecutionContext());
        // Perform INSERT operation - match the 16 columns of incremental_order_header table
        dbConnection.createStatement().execute("INSERT INTO tb_101.raw_pos.incremental_order_header "
                + "(order_id, truck_id, location_id, customer_id, discount_id, shift_id, "
                + "shift_start_time, shift_end_time, order_channel, order_ts, served_ts, "
                + "order_currency, order_amount, order_tax_amount, order_discount_amount, order_total) "
                + "VALUES (9999999, 999, 1.0, 12345, 'DISCOUNT_01', 99999, "
                + "'09:00:00', '17:00:00', 'APP', CURRENT_TIMESTAMP(), '2023-01-01 12:00:00', "
                + "'USD', 25.50, '2.50', '0.00', 28.00)");
        SnowflakeStreamRow createRow = reader.poll(10, TimeUnit.SECONDS);
        Assertions.assertEquals(SnowflakeStreamRow.Action.INSERT, createRow.getAction());
        Assertions.assertFalse(createRow.isUpdate());

        // Perform UPDATE operation
        dbConnection.createStatement().execute(
                "UPDATE tb_101.raw_pos.incremental_order_header SET location_id = 2.0, truck_id = 888 "
                        + "WHERE order_id = 4063758");
        SnowflakeStreamRow updateRow = reader.poll(10, TimeUnit.SECONDS);
        Assertions.assertEquals(SnowflakeStreamRow.Action.INSERT, updateRow.getAction());
        Assertions.assertTrue(updateRow.isUpdate());
        // Perform DELETE operation
        dbConnection.createStatement().execute("DELETE FROM tb_101.raw_pos.incremental_order_header WHERE order_id = 4063759");
        SnowflakeStreamRow deleteRow = reader.poll(10, TimeUnit.SECONDS);
        Assertions.assertEquals(SnowflakeStreamRow.Action.DELETE, deleteRow.getAction());

    }

}
