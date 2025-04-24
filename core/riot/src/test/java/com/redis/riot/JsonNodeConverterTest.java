package com.redis.riot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class JsonNodeConverterTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testConvertJsonNode_withPrimitiveValues() throws Exception {
        JsonNode stringNode = objectMapper.readTree("\"test\"");
        JsonNode numberNode = objectMapper.readTree("123");
        JsonNode booleanNode = objectMapper.readTree("true");
        JsonNode nullNode = objectMapper.readTree("null");

        assertEquals("test", JsonNodeConverter.convertJsonNode(stringNode));
        assertEquals(123, JsonNodeConverter.convertJsonNode(numberNode));
        assertEquals(true, JsonNodeConverter.convertJsonNode(booleanNode));
        assertNull(JsonNodeConverter.convertJsonNode(nullNode));
    }

    @Test
    void testConvertJsonNode_withArray() throws Exception {
        JsonNode arrayNode = objectMapper.readTree("[\"value1\", 42, false]");

        Object result = JsonNodeConverter.convertJsonNode(arrayNode);
        assertTrue(result instanceof List);

        List<?> list = (List<?>) result;
        assertEquals(3, list.size());
        assertEquals("value1", list.get(0));
        assertEquals(42, list.get(1));
        assertEquals(false, list.get(2));
    }

    @Test
    void testConvertJsonNode_withObject() throws Exception {
        JsonNode objectNode = objectMapper.readTree("{\"key1\": \"value1\", \"key2\": 42, \"key3\": true}");

        Object result = JsonNodeConverter.convertJsonNode(objectNode);
        assertTrue(result instanceof Map);

        Map<?, ?> map = (Map<?, ?>) result;
        assertEquals(3, map.size());
        assertEquals("value1", map.get("key1"));
        assertEquals(42, map.get("key2"));
        assertEquals(true, map.get("key3"));
    }

    @Test
    void testConvertJsonNode_withNestedStructure() throws Exception {
        JsonNode nestedNode = objectMapper.readTree("{\"key1\": [\"value1\", 42], \"key2\": {\"nestedKey\": false}}");

        Object result = JsonNodeConverter.convertJsonNode(nestedNode);
        assertTrue(result instanceof Map);

        Map<?, ?> map = (Map<?, ?>) result;
        assertEquals(2, map.size());

        assertTrue(map.get("key1") instanceof List);
        List<?> list = (List<?>) map.get("key1");
        assertEquals("value1", list.get(0));
        assertEquals(42, list.get(1));

        assertTrue(map.get("key2") instanceof Map);
        Map<?, ?> nestedMap = (Map<?, ?>) map.get("key2");
        assertEquals(false, nestedMap.get("nestedKey"));
    }

    @Test
    void testConvertJsonNode_topLevelArray() throws Exception {
        String jsonArray = "[{\"key1\": \"value1\"}, [1, 2, 3], \"string\", 42, true]";

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(jsonArray);

        Object result = JsonNodeConverter.convertJsonNode(jsonNode);
        assertInstanceOf(List.class, result);

        List<Object> resultList = (List<Object>) result;
        assert(resultList.size() == 5);

        assertInstanceOf(Map.class, resultList.get(0));
        assertEquals("value1", ((Map<?, ?>) resultList.get(0)).get("key1"));

        assertInstanceOf(List.class, resultList.get(1));
        List<?> innerList = (List<?>) resultList.get(1);
        assertEquals(3, innerList.size());
        assertEquals(1, innerList.get(0));
        assertEquals(2, innerList.get(1));
        assertEquals(3, innerList.get(2));

        assertEquals("string", resultList.get(2));
        assertEquals(42, resultList.get(3));
        assertEquals(true, resultList.get(4));
    }
}
