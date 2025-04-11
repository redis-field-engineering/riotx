package com.redis.riot;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;

public abstract class ExportStepHelper {

    public static final String NOTIFY_CONFIG = "notify-keyspace-events";

    public static final String NOTIFY_CONFIG_VALUE = "KEA";

    private ExportStepHelper() {
    }

    public static void checkNotifyConfig(AbstractRedisClient client, Logger log) {
        Map<String, String> valueMap;
        try (StatefulRedisModulesConnection<String, String> conn = BatchUtils.connection(client)) {
            try {
                valueMap = conn.sync().configGet(NOTIFY_CONFIG);
            } catch (RedisException e) {
                log.info("Could not check keyspace notification config", e);
                return;
            }
        }
        String actual = valueMap.getOrDefault(NOTIFY_CONFIG, "");
        log.info("Retrieved config {}: {}", NOTIFY_CONFIG, actual);
        Set<Character> expected = characterSet(NOTIFY_CONFIG_VALUE);
        Assert.isTrue(characterSet(actual).containsAll(expected),
                String.format("Keyspace notifications not property configured. Expected %s '%s' but was '%s'.", NOTIFY_CONFIG,
                        NOTIFY_CONFIG_VALUE, actual));
    }

    private static Set<Character> characterSet(String string) {
        return string.codePoints().mapToObj(c -> (char) c).collect(Collectors.toSet());
    }

}
