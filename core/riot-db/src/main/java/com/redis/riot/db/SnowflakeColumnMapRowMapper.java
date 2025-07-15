package com.redis.riot.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.redis.riot.core.JsonNodeConverter.convertJsonNode;

public class SnowflakeColumnMapRowMapper extends ColumnMapRowMapper {

    private static final String TYPE_VARIANT = "VARIANT";

    private final ObjectMapper mapper = new ObjectMapper();

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    protected Object getColumnValue(ResultSet rs, int index) throws SQLException {
        Object value = super.getColumnValue(rs, index);
        if (value != null) {
            String columnTypeName = rs.getMetaData().getColumnTypeName(index);
            if (TYPE_VARIANT.equalsIgnoreCase(columnTypeName)) {
                String valueString = String.valueOf(value);
                try {
                    JsonNode jsonNode = mapper.readTree(valueString);
                    value = convertJsonNode(jsonNode);
                } catch (JsonProcessingException e) {
                    log.debug("Could not parse VARIANT value as JSON: {}", value);
                }
            }
        }
        return value;
    }

}
