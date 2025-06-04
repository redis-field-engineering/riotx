package com.redis.spring.batch.test;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.enterprise.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisContainer;

public interface RedisContainerFactory {

    static RedisContainer redis() {
        return redis(RedisContainer.DEFAULT_TAG);
    }

    @SuppressWarnings("resource")
    static RedisEnterpriseContainer enterprise() {
        return new RedisEnterpriseContainer(
                RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag(RedisEnterpriseContainer.DEFAULT_TAG)).withDatabase(
                Database.builder().name("BatchTests").memoryMB(50).ossCluster(true)
                        .modules(RedisModule.JSON, RedisModule.SEARCH).build());
    }

    static RedisContainer redis(String tag) {
        return new RedisContainer(RedisContainer.DEFAULT_IMAGE_NAME.withTag(tag));
    }

}
