package com.redis.riot;

import com.redis.riot.db.DataSourceBuilder;
import lombok.ToString;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@ToString
public class DataSourceArgs {

    @Option(names = "--jdbc-driver", description = "Fully qualified name of the JDBC driver (e.g. oracle.jdbc.OracleDriver).", paramLabel = "<class>")
    private Class<?> driver;

    @Option(names = "--jdbc-url", required = true, description = "JDBC URL to connect to the database.", paramLabel = "<string>")
    private String url;

    @Option(names = "--jdbc-user", description = "Login username of the database.", paramLabel = "<string>")
    private String username;

    @Option(names = "--jdbc-pass", arity = "0..1", interactive = true, description = "Login password of the database.", paramLabel = "<pwd>")
    private String password;

    @Option(names = "--jdbc-property", description = "Additional JDBC property (key=value).", paramLabel = "<prop>")
    private Map<String, String> properties = new HashMap<>();

    @Option(names = "--jdbc-pool", description = "Maximum number of connections in the pool.", paramLabel = "<int>")
    private Integer maxPoolSize;

    @Option(names = "--jdbc-min-idle", description = "Minimum number of idle connections in the pool.", hidden = true)
    private Integer minIdle;

    @Option(names = "--jdbc-timeout", description = "Maximum time to wait for a connection from the pool.", paramLabel = "<dur>")
    private Duration connectionTimeout;

    @Option(names = "--jdbc-idle-timeout", description = "Maximum idle time for a connection in the pool.", hidden = true)
    private Duration idleTimeout;

    @Option(names = "--jdbc-max-lifetime", description = "Maximum lifetime of a connection in the pool.", hidden = true)
    private Duration maxLifetime;

    @Option(names = "--jdbc-initialization-fail-timeout", description = "Connection initialization failure timeout.", hidden = true)
    private Duration initializationFailTimeout;

    @Option(names = "--jdbc-validation-timeout", description = "Validation timeout for connection checking.", hidden = true)
    private Duration validationTimeout;

    @Option(names = "--jdbc-leak-detection-threshold", description = "Time threshold for leak detection.", hidden = true)
    private Duration leakDetectionThreshold;

    @Option(names = "--jdbc-auto-commit", description = "Whether auto-commit is enabled for connections in the pool.", hidden = true)
    private Boolean autoCommit;

    @Option(names = "--jdbc-pool-name", description = "Custom name for the connection pool.", paramLabel = "<name>", hidden = true)
    private String poolName;

    public DataSourceBuilder dataSourceBuilder() {
        DataSourceBuilder builder = new DataSourceBuilder();
        builder.driver(driver == null ? null : driver.getName());
        builder.url(url);
        builder.username(username);
        builder.password(password);
        builder.properties(properties);
        builder.maximumPoolSize(maxPoolSize);
        builder.minimumIdle(minIdle);
        builder.connectionTimeout(connectionTimeout);
        builder.idleTimeout(idleTimeout);
        builder.maxLifetime(maxLifetime);
        builder.initializationFailTimeout(initializationFailTimeout);
        builder.validationTimeout(validationTimeout);
        builder.leakDetectionThreshold(leakDetectionThreshold);
        builder.autoCommit(autoCommit);
        builder.poolName(poolName);
        return builder;
    }

    public Class<?> getDriver() {
        return driver;
    }

    public void setDriver(Class<?> driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Integer getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(Integer maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public Integer getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(Integer minIdle) {
        this.minIdle = minIdle;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public Duration getMaxLifetime() {
        return maxLifetime;
    }

    public void setMaxLifetime(Duration maxLifetime) {
        this.maxLifetime = maxLifetime;
    }

    public Duration getInitializationFailTimeout() {
        return initializationFailTimeout;
    }

    public void setInitializationFailTimeout(Duration initializationFailTimeout) {
        this.initializationFailTimeout = initializationFailTimeout;
    }

    public Duration getValidationTimeout() {
        return validationTimeout;
    }

    public void setValidationTimeout(Duration validationTimeout) {
        this.validationTimeout = validationTimeout;
    }

    public Duration getLeakDetectionThreshold() {
        return leakDetectionThreshold;
    }

    public void setLeakDetectionThreshold(Duration leakDetectionThreshold) {
        this.leakDetectionThreshold = leakDetectionThreshold;
    }

    public Boolean getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(Boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

}
