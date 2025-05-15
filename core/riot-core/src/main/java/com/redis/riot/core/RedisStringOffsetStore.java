package com.redis.riot.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.utils.ConnectionBuilder;
import io.lettuce.core.AbstractRedisClient;

import java.util.Map;

public class RedisStringOffsetStore implements OffsetStore {

    private final ObjectMapper mapper = new ObjectMapper();

    private final String key;

    private final StatefulRedisModulesConnection<String, String> connection;

    public RedisStringOffsetStore(AbstractRedisClient client, String key) {
        this.connection = ConnectionBuilder.client(client).connection();
        this.key = key;
    }

    @Override
    public Map<String, Object> getOffset() throws Exception {
        String json = connection.sync().get(key);
        if (json == null) {
            return null;
        }
        return mapper.readValue(json, Map.class);
    }

    @Override
    public void store(Map<String, Object> offset) throws Exception {
        connection.sync().set(key, mapper.writeValueAsString(offset));
    }

}
