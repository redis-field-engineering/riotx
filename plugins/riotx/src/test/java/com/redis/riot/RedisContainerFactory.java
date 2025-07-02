package com.redis.riot;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.enterprise.testcontainers.RedisEnterpriseContainer;
import com.redis.enterprise.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisContainer;
import org.testcontainers.utility.DockerImageName;

public class RedisContainerFactory {

    private RedisContainerFactory() {
    }

    public static RedisContainer valkey() {
        return new RedisContainer(DockerImageName.parse("valkey/valkey").withTag("8.0.0"));
    }

    public static RedisContainer redis() {
        return new RedisContainer(RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));
    }

    @SuppressWarnings("resource")
    public static RedisEnterpriseContainer enterprise() {
        return new RedisEnterpriseContainer(
                RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag(RedisEnterpriseContainer.DEFAULT_TAG)).withDatabase(
                Database.builder().name("BatchTests").memoryMB(50).ossCluster(true)
                        .modules(RedisModule.TIMESERIES, RedisModule.JSON, RedisModule.SEARCH).build());
    }

    public static RedisEnterpriseServer enterpriseServer() {
        RedisEnterpriseServer server = new RedisEnterpriseServer();
        server.withDatabase(Database.builder().shardCount(2).port(12001).ossCluster(true)
                .modules(RedisModule.JSON, RedisModule.SEARCH, RedisModule.TIMESERIES).build());
        return server;
    }

}
