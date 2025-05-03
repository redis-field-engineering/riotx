package com.redis.spring.batch.item.redis.common;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import com.redis.lettucemod.utils.ConnectionBuilder;
import io.lettuce.core.AbstractRedisClient;

public class RedisInfo {

    public static final String SERVER_NAME = "server_name";

    public static final String OS = "os";

    private final Properties properties;

    public RedisInfo(Properties properties) {
        this.properties = properties;
    }

    public String getOS() {
        return getProperty(OS);
    }

    public String getServerName() {
        return getProperty(SERVER_NAME);
    }

    private String getProperty(String key) {
        return properties.getProperty(key);
    }

    @Override
    public String toString() {
        return properties.toString();
    }

    public static RedisInfo from(AbstractRedisClient client) {
        Properties properties = new Properties();
        try (StatefulRedisModulesConnection<String, String> connection = ConnectionBuilder.client(client).connection()) {
            try {
                properties.load(new StringReader(connection.sync().info("server")));
            } catch (IOException e) {
                // ignore
            }
        }
        return new RedisInfo(properties);
    }

}
