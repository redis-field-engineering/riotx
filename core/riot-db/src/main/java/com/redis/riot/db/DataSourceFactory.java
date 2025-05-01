package com.redis.riot.db;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.jdbc.EmbeddedDatabaseConnection;

import javax.sql.DataSource;

public class DataSourceFactory {

    public static DataSource create(String driver, String url, String username, String password) {
        DataSourceProperties properties = new DataSourceProperties();
        properties.setDriverClassName(driver);
        properties.setPassword(password);
        properties.setUrl(url);
        properties.setUsername(username);
        properties.setEmbeddedDatabaseConnection(EmbeddedDatabaseConnection.NONE);
        return properties.initializeDataSourceBuilder().build();
    }

}
