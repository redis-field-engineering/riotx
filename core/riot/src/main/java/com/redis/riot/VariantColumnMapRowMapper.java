package com.redis.riot;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import static com.redis.riot.JsonNodeConverter.convertJsonNode;

public class VariantColumnMapRowMapper implements RowMapper<Map<String, Object>> {

    private static final String TYPE_VARIANT = "VARIANT";

    private final ObjectMapper mapper = new ObjectMapper();
    private final Logger log = org.slf4j.LoggerFactory.getLogger(getClass());

    public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        Map<String, Object> mapOfColumnValues = this.createColumnMap(columnCount);

        for(int i = 1; i <= columnCount; ++i) {
            String column = JdbcUtils.lookupColumnName(rsmd, i);
            mapOfColumnValues.putIfAbsent(getColumnKey(column), getColumnValue(rs, i));
        }

        return mapOfColumnValues;
    }

    protected Map<String, Object> createColumnMap(int columnCount) {
        return new LinkedCaseInsensitiveMap<>(columnCount);
    }

    protected String getColumnKey(String columnName) {
        return columnName;
    }

    @Nullable
    protected Object getColumnValue(ResultSet rs, int index) throws SQLException {
        Object value = JdbcUtils.getResultSetValue(rs, index);
        if (value==null) {
            return null;
        }

        String columnTypeName = rs.getMetaData().getColumnTypeName(index);
        if (TYPE_VARIANT.equalsIgnoreCase(columnTypeName)) {
            String valueString = String.valueOf(value);
            try {
                JsonNode jsonNode = mapper.readTree(valueString);
                value = convertJsonNode(jsonNode);
                return value;
            } catch (JsonProcessingException e) {
                log.debug("Could not parse VARIANT value as JSON: " + value);
                return value;
            }
        }
        return value;
    }


}
