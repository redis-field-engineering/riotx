package com.redis.riot.rdi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.riot.core.OffsetStore;
import io.lettuce.core.AbstractRedisClient;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class RdiOffsetStore implements OffsetStore {

    public static final String DEFAULT_KEY = "metadata:debezium:offsets";

    public static final String DEFAULT_SERVER = "rdi";

    private final ObjectMapper mapper = new ObjectMapper();

    private final AbstractRedisClient client;

    private final String table;

    private String key = DEFAULT_KEY;

    private String server = DEFAULT_SERVER;

    public RdiOffsetStore(AbstractRedisClient client, String table) {
        this.client = client;
        this.table = table;
    }

    private StatefulRedisModulesConnection<String, String> connection() {
        return ConnectionBuilder.client(client).connection();
    }

    @Override
    public void store(Map<String, Object> offset) throws JsonProcessingException {
        try (StatefulRedisModulesConnection<String, String> connection = connection()) {
            connection.sync().hset(key, field(), mapper.writeValueAsString(offset));
        }
    }

    private String field() throws JsonProcessingException {
        Map<String, String> field = new HashMap<>();
        field.put("server", server);
        field.put("table", table);
        return mapper.writeValueAsString(field);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> getOffset() throws JsonProcessingException {
        try (StatefulRedisModulesConnection<String, String> connection = connection()) {
            String value = connection.sync().hget(key, field());
            if (StringUtils.hasLength(value)) {
                return mapper.readValue(value, Map.class);
            }
            return null;
        }
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

}
