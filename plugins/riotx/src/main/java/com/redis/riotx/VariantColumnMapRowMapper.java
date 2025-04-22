package com.redis.riotx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.redis.riot.core.RiotException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VariantColumnMapRowMapper implements RowMapper<Map<String, Object>> {
    private ObjectMapper mapper;

    public VariantColumnMapRowMapper() {
        this.mapper = new ObjectMapper();
    }

    public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        Map<String, Object> mapOfColumnValues = this.createColumnMap(columnCount);

        for(int i = 1; i <= columnCount; ++i) {
            String column = JdbcUtils.lookupColumnName(rsmd, i);
            int columnType = rsmd.getColumnType(i);
            String columnTypeName = rsmd.getColumnTypeName(i);

            Object resultValue = this.getColumnValue(rs, i);
            Object value = null;

            if (resultValue != null && columnType == Types.VARCHAR && "VARIANT".equalsIgnoreCase(columnTypeName)){
                try {
                    String resultString = resultValue.toString();

                    // naive check for JSON array or object - VARIANTs can store other types
                    if (resultString.startsWith("[")){
                        value = mapper.readValue(resultString, new TypeReference<List<Object>>() {});
                    } else if (resultString.startsWith("{")){
                        value = mapper.readValue(resultString, new TypeReference<>() {});
                    } else {
                        value = resultString;
                    }
                } catch (JsonProcessingException e) {
                    throw new RiotException("Could not parse VARIANT column " + column + " = " + resultValue.toString()  , e);
                }
            } else {
                value = resultValue;
            }

            mapOfColumnValues.putIfAbsent(this.getColumnKey(column), value);
        }

        return mapOfColumnValues;
    }

    protected Map<String, Object> createColumnMap(int columnCount) {
        return new LinkedCaseInsensitiveMap(columnCount);
    }

    protected String getColumnKey(String columnName) {
        return columnName;
    }

    @Nullable
    protected Object getColumnValue(ResultSet rs, int index) throws SQLException {
        return JdbcUtils.getResultSetValue(rs, index);
    }

}
