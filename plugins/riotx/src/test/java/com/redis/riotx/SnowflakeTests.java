package com.redis.riotx;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

import com.redis.riot.DataSourceArgs;
import com.redis.riot.SqlScriptRunner;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

import picocli.CommandLine;

@EnabledIfEnvironmentVariable(named = "JDBC_ADMIN_PASSWORD", matches = ".+")
class SnowflakeTests extends AbstractRiotxApplicationTestBase {

    private static final RedisStackContainer redis = new RedisStackContainer(
            RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

    protected Connection dbConnection;

    protected DataSource dataSource;

    private SqlScriptRunner sqlRunner;

    private static final String DEFAULT_URL = "jdbc:snowflake://NYENSDO-XR16179.snowflakecomputing.com";

    private String getenv(String name, String defaultVal) {
        String val = System.getenv(name);
        if (val != null) {
            return val;
        } else if (defaultVal != null) {
            return defaultVal;
        } else {
            throw new RuntimeException(String.format("Required Environment variable %s not set", name));
        }
    }

    private DataSourceProperties getUnitTestDbProperties() {
        DataSourceProperties properties = new DataSourceProperties();
        properties.setUrl(getenv("JDBC_URL", DEFAULT_URL));
        properties.setUsername(getenv("JDBC_ADMIN_USERNAME", "jplichta"));
        properties.setPassword(getenv("JDBC_ADMIN_PASSWORD", null));
        properties.setDriverClassName(SnowflakeImport.JDBC_DRIVER);
        return properties;
    }

    private DataSourceProperties getRiotDbProperties() {
        DataSourceProperties properties = new DataSourceProperties();
        properties.setUrl(getenv("JDBC_URL", DEFAULT_URL));
        properties.setUsername(getenv("JDBC_USERNAME", "riotx_cdc"));
        properties.setPassword(getenv("JDBC_PASSWORD", null));
        properties.setDriverClassName(SnowflakeImport.JDBC_DRIVER);
        return properties;
    }

    @BeforeAll
    public void setupConnection() throws Exception {
        DataSourceProperties properties = getUnitTestDbProperties();
        properties.afterPropertiesSet();
        dataSource = properties.initializeDataSourceBuilder().build();
        dbConnection = dataSource.getConnection();
        sqlRunner = new SqlScriptRunner(dbConnection, Map.of("{{PASSWORD}}", getenv("JDBC_PASSWORD", null)));
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

        sqlRunner.executeScript("db/snowflake-insert-more-data.sql");
        execute(info, "snowflake-import", this::executeSnowflakeImport);

        try (Statement statement = dbConnection.createStatement()) {
            statement.execute("SELECT COUNT(*) AS count FROM tb_101.raw_pos.incremental_order_header");
            ResultSet resultSet = statement.getResultSet();
            resultSet.next();

            long count = resultSet.getLong(1);

            Assertions.assertEquals(1100, count);
            Assertions.assertEquals(count, keyCount("orderheader:*"));

            Map<String, String> order = redisCommands.hgetall("orderheader:4064758");
            Assertions.assertEquals("18", order.get("TRUCK_ID"));
            Assertions.assertEquals("21207", order.get("SHIFT_ID"));
        }
    }

    protected int executeSnowflakeImport(CommandLine.ParseResult parseResult) {
        SnowflakeImport command = command(parseResult);
        configureDatabase(command.getDataSourceArgs());
        return CommandLine.ExitCode.OK;
    }

    private void configureDatabase(DataSourceArgs args) {
        DataSourceProperties properties = getUnitTestDbProperties();
        args.setUrl(properties.getUrl());
        args.setUsername(properties.getUsername());
        args.setPassword(properties.getPassword());
    }

}
