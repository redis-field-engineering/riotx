package com.redis.riot.db;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SnowflakeColumnMapRowMapperTests {

    @Mock
    private ResultSet resultSet;

    @Mock
    private ResultSetMetaData metaData;

    private SnowflakeColumnMapRowMapper mapper;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        mapper = new SnowflakeColumnMapRowMapper();
        when(resultSet.getMetaData()).thenReturn(metaData);
    }

    @Test
    void testGetColumnValueWithNullValue() throws SQLException {
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("test_column");
        when(metaData.getColumnTypeName(1)).thenReturn("VARCHAR");
        when(resultSet.getObject(1)).thenReturn(null);

        Map<String, Object> result = mapper.mapRow(resultSet, 1);

        assertNotNull(result);
        assertNull(result.get("test_column"));
    }

    @Test
    void testGetColumnValueWithNonVariantType() throws SQLException {
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("text_column");
        when(metaData.getColumnTypeName(1)).thenReturn("VARCHAR");
        when(resultSet.getObject(1)).thenReturn("test_value");

        Map<String, Object> result = mapper.mapRow(resultSet, 1);

        assertNotNull(result);
        assertEquals("test_value", result.get("text_column"));
    }

    @Test
    void testGetColumnValueWithVariantTypeValidJson() throws SQLException {
        String jsonString = "{\"name\":\"John\",\"age\":30,\"active\":true}";
        
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("variant_column");
        when(metaData.getColumnTypeName(1)).thenReturn("VARIANT");
        when(resultSet.getObject(1)).thenReturn(jsonString);

        Map<String, Object> result = mapper.mapRow(resultSet, 1);

        assertNotNull(result);
        Object variantValue = result.get("variant_column");
        assertInstanceOf(Map.class, variantValue);
        
        @SuppressWarnings("unchecked")
        Map<String, Object> jsonMap = (Map<String, Object>) variantValue;
        assertEquals("John", jsonMap.get("name"));
        assertEquals(30, jsonMap.get("age"));
        assertEquals(true, jsonMap.get("active"));
    }

    @Test
    void testGetColumnValueWithVariantTypeJsonArray() throws SQLException {
        String jsonString = "[\"item1\",\"item2\",\"item3\"]";
        
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("variant_array");
        when(metaData.getColumnTypeName(1)).thenReturn("VARIANT");
        when(resultSet.getObject(1)).thenReturn(jsonString);

        Map<String, Object> result = mapper.mapRow(resultSet, 1);

        assertNotNull(result);
        Object variantValue = result.get("variant_array");
        assertInstanceOf(List.class, variantValue);
        
        @SuppressWarnings("unchecked")
        List<Object> jsonList = (List<Object>) variantValue;
        assertEquals(3, jsonList.size());
        assertEquals("item1", jsonList.get(0));
        assertEquals("item2", jsonList.get(1));
        assertEquals("item3", jsonList.get(2));
    }

    @Test
    void testGetColumnValueWithVariantTypeInvalidJson() throws SQLException {
        String invalidJson = "not_valid_json";
        
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("variant_column");
        when(metaData.getColumnTypeName(1)).thenReturn("VARIANT");
        when(resultSet.getObject(1)).thenReturn(invalidJson);

        Map<String, Object> result = mapper.mapRow(resultSet, 1);

        assertNotNull(result);
        // Should return the original string value when JSON parsing fails
        assertEquals(invalidJson, result.get("variant_column"));
    }

    @Test
    void testGetColumnValueWithVariantTypeCaseInsensitive() throws SQLException {
        String jsonString = "{\"test\":\"value\"}";
        
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("variant_column");
        when(metaData.getColumnTypeName(1)).thenReturn("variant"); // lowercase
        when(resultSet.getObject(1)).thenReturn(jsonString);

        Map<String, Object> result = mapper.mapRow(resultSet, 1);

        assertNotNull(result);
        Object variantValue = result.get("variant_column");
        assertInstanceOf(Map.class, variantValue);
        
        @SuppressWarnings("unchecked")
        Map<String, Object> jsonMap = (Map<String, Object>) variantValue;
        assertEquals("value", jsonMap.get("test"));
    }

    @Test
    void testGetColumnValueWithMultipleColumns() throws SQLException {
        when(metaData.getColumnCount()).thenReturn(3);
        
        // Regular VARCHAR column
        when(metaData.getColumnLabel(1)).thenReturn("name");
        when(metaData.getColumnTypeName(1)).thenReturn("VARCHAR");
        when(resultSet.getObject(1)).thenReturn("John Doe");
        
        // VARIANT column with JSON
        when(metaData.getColumnLabel(2)).thenReturn("metadata");
        when(metaData.getColumnTypeName(2)).thenReturn("VARIANT");
        when(resultSet.getObject(2)).thenReturn("{\"age\":25}");
        
        // NUMBER column
        when(metaData.getColumnLabel(3)).thenReturn("id");
        when(metaData.getColumnTypeName(3)).thenReturn("NUMBER");
        when(resultSet.getObject(3)).thenReturn(123);

        Map<String, Object> result = mapper.mapRow(resultSet, 1);

        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("John Doe", result.get("name"));
        assertEquals(123, result.get("id"));
        
        Object metadata = result.get("metadata");
        assertInstanceOf(Map.class, metadata);
        @SuppressWarnings("unchecked")
        Map<String, Object> metadataMap = (Map<String, Object>) metadata;
        assertEquals(25, metadataMap.get("age"));
    }

    @Test
    void testGetColumnValueWithVariantTypeNull() throws SQLException {
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("variant_column");
        when(metaData.getColumnTypeName(1)).thenReturn("VARIANT");
        when(resultSet.getObject(1)).thenReturn(null);

        Map<String, Object> result = mapper.mapRow(resultSet, 1);

        assertNotNull(result);
        assertNull(result.get("variant_column"));
    }
}