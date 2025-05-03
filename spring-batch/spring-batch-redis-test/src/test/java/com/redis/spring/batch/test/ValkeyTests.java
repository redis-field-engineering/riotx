package com.redis.spring.batch.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.spring.batch.item.redis.common.RedisInfo;
import com.redis.spring.batch.item.redis.common.RedisSupportCheck;

import io.lettuce.core.RedisURI;

@TestInstance(Lifecycle.PER_CLASS)
public class ValkeyTests {

    @SuppressWarnings("resource")
    private GenericContainer<?> valkey = new GenericContainer<>("valkey/valkey").withExposedPorts(6379);

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @BeforeAll
    void setup() throws Exception {
        valkey.start();
    }

    @AfterAll
    void teardown() {
        valkey.stop();
    }

    @Test
    void testRedisSupport() {
        RedisModulesClient client = RedisModulesClient.create(RedisURI.create(valkey.getHost(), valkey.getFirstMappedPort()));
        RedisSupportCheck check = new RedisSupportCheck();
        check.getConsumers().clear();
        List<RedisInfo> infoList = new ArrayList<>();
        check.getConsumers().add(infoList::add);
        check.accept(client);
        Assertions.assertEquals(1, infoList.size());
        client.shutdown();
    }

}
