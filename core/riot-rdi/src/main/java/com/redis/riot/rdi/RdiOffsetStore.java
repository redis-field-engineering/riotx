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

    private final StatefulRedisModulesConnection<String, String> connection;

    private final String table;

    private String key = DEFAULT_KEY;

    private String server = DEFAULT_SERVER;

    public RdiOffsetStore(StatefulRedisModulesConnection<String, String> connection, String table) {
        this.connection = connection;
        this.table = table;
    }

    @Override
    public void store(Map<String, Object> offset) throws JsonProcessingException {
        connection.sync().hset(key, field(), mapper.writeValueAsString(offset));
    }

    @Override
    public void clear() {
        connection.sync().del(key);
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
        String value = connection.sync().hget(key, field());
        if (StringUtils.hasLength(value)) {
            return mapper.readValue(value, Map.class);
        }
        return null;
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
