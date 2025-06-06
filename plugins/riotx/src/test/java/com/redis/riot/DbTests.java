package com.redis.riot;

import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.testcontainers.containers.JdbcDatabaseContainer;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.ParseResult;

import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;

abstract class DbTests extends AbstractRiotApplicationTestBase {

    private static final RedisContainer redis = new RedisContainer(
            RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));

    protected Connection dbConnection;

    protected DataSource dataSource;

    protected abstract JdbcDatabaseContainer<?> getJdbcDatabaseContainer();

    @BeforeAll
    public void setupContainers() throws SQLException {
        JdbcDatabaseContainer<?> container = getJdbcDatabaseContainer();
        container.start();
        DataSourceProperties properties = new DataSourceProperties();
        properties.setUrl(container.getJdbcUrl());
        properties.setUsername(container.getUsername());
        properties.setPassword(container.getPassword());
        dataSource = properties.initializeDataSourceBuilder().build();
        dbConnection = dataSource.getConnection();
    }

    @AfterAll
    public void teardownContainers() throws SQLException {
        if (dbConnection != null) {
            dbConnection.close();
        }
        getJdbcDatabaseContainer().stop();
    }

    @Override
    protected RedisServer getRedisServer() {
        return redis;
    }

    @Override
    protected RedisContainer getTargetRedisServer() {
        return redis;
    }

    protected void executeScript(String file) throws IOException, SQLException {
        SqlScriptRunner scriptRunner = new SqlScriptRunner(dbConnection);
        scriptRunner.setAutoCommit(false);
        scriptRunner.setStopOnError(true);
        InputStream inputStream = PostgresTests.class.getClassLoader().getResourceAsStream(file);
        if (inputStream == null) {
            throw new FileNotFoundException(file);
        }
        scriptRunner.runScript(new InputStreamReader(inputStream));
    }

    protected int executeDatabaseImport(ParseResult parseResult) {
        DatabaseImport command = command(parseResult);
        configureDatabase(command.getDataSourceArgs());
        return ExitCode.OK;
    }

    protected int executeDatabaseExport(ParseResult parseResult, TestInfo info) {
        DatabaseExport command = command(parseResult);
        configureDatabase(command.getDataSourceArgs());
        return ExitCode.OK;
    }

    private void configureDatabase(DataSourceArgs args) {
        JdbcDatabaseContainer<?> container = getJdbcDatabaseContainer();
        args.setUrl(container.getJdbcUrl());
        args.setUsername(container.getUsername());
        args.setPassword(container.getPassword());
    }

}
