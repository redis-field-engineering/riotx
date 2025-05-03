package com.redis.riot.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.jdbc.DatabaseDriver;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class DataSourceBuilder {

    private String driver;

    private String url;

    private String username;

    private String password;

    private final Map<String, String> properties = new HashMap<>();

    // HikariCP-specific fields
    private Integer maximumPoolSize;

    private Integer minimumIdle;

    private Duration connectionTimeout;

    private Duration idleTimeout;

    private Duration maxLifetime;

    private Duration initializationFailTimeout;

    private Duration validationTimeout;

    private Duration leakDetectionThreshold;

    private Boolean autoCommit;

    private String poolName;

    public String url() {
        return url;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public String driver() {
        return driver;
    }

    public DataSourceBuilder driver(String driver) {
        this.driver = driver;
        return this;
    }

    public DataSourceBuilder url(String url) {
        this.url = url;
        return this;
    }

    public DataSourceBuilder username(String username) {
        this.username = username;
        return this;
    }

    public DataSourceBuilder password(String password) {
        this.password = password;
        return this;
    }

    public DataSourceBuilder property(String key, String value) {
        this.properties.put(key, value);
        return this;
    }

    public DataSourceBuilder properties(Map<String, String> properties) {
        this.properties.putAll(properties);
        return this;
    }

    public DataSourceBuilder maximumPoolSize(Integer max) {
        this.maximumPoolSize = max;
        return this;
    }

    public DataSourceBuilder minimumIdle(Integer min) {
        this.minimumIdle = min;
        return this;
    }

    public DataSourceBuilder connectionTimeout(Duration timeout) {
        this.connectionTimeout = timeout;
        return this;
    }

    public DataSourceBuilder idleTimeout(Duration timeout) {
        this.idleTimeout = timeout;
        return this;
    }

    public DataSourceBuilder maxLifetime(Duration lifetime) {
        this.maxLifetime = lifetime;
        return this;
    }

    public DataSourceBuilder initializationFailTimeout(Duration timeout) {
        this.initializationFailTimeout = timeout;
        return this;
    }

    public DataSourceBuilder validationTimeout(Duration timeout) {
        this.validationTimeout = timeout;
        return this;
    }

    public DataSourceBuilder leakDetectionThreshold(Duration threshold) {
        this.leakDetectionThreshold = threshold;
        return this;
    }

    public DataSourceBuilder autoCommit(Boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    public DataSourceBuilder poolName(String poolName) {
        this.poolName = poolName;
        return this;
    }

    public DataSource build() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(driverClassName());
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);

        if (maximumPoolSize != null) {
            config.setMaximumPoolSize(maximumPoolSize);
        }
        if (minimumIdle != null) {
            config.setMinimumIdle(minimumIdle);
        }
        if (connectionTimeout != null) {
            config.setConnectionTimeout(connectionTimeout.toMillis());
        }
        if (idleTimeout != null) {
            config.setIdleTimeout(idleTimeout.toMillis());
        }
        if (maxLifetime != null) {
            config.setMaxLifetime(maxLifetime.toMillis());
        }
        if (initializationFailTimeout != null) {
            config.setInitializationFailTimeout(initializationFailTimeout.toMillis());
        }
        if (validationTimeout != null) {
            config.setValidationTimeout(validationTimeout.toMillis());
        }
        if (leakDetectionThreshold != null) {
            config.setLeakDetectionThreshold(leakDetectionThreshold.toMillis());
        }
        if (autoCommit != null) {
            config.setAutoCommit(autoCommit);
        }
        if (poolName != null) {
            config.setPoolName(poolName);
        }

        // JDBC vendor-specific properties
        properties.forEach(config::addDataSourceProperty);

        return new HikariDataSource(config);
    }

    private String driverClassName() {
        if (StringUtils.hasLength(driver)) {
            return driver;
        }
        return DatabaseDriver.fromJdbcUrl(url).getDriverClassName();
    }

    public Integer maximumPoolSize() {
        return maximumPoolSize;
    }

    public Integer minimumIdle() {
        return minimumIdle;
    }

}
