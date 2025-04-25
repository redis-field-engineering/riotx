package com.redis.riot;

import org.springframework.jdbc.datasource.DelegatingDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class InitSqlDataSource extends DelegatingDataSource {

    private final List<String> initSqlStatements;

    public InitSqlDataSource(DataSource targetDataSource, List<String> initSqlStatements) {
        super(targetDataSource);
        this.initSqlStatements = initSqlStatements;
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = super.getConnection();
        try (Statement stmt = connection.createStatement()) {
            for (String sql : initSqlStatements) {
                stmt.execute(sql);
            }
        }
        return connection;
    }

}
