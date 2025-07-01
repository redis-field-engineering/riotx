package com.redis.batch;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class KeyStructEventTest {

    @Test
    void testKeyStructEvent() {
        // Create a KeyStructEvent
        KeyStructEvent<String, String> event = new KeyStructEvent<>();
        event.setKey("test:key");
        event.setType(KeyType.hash);

        Map<String, String> hashValue = new HashMap<>();
        hashValue.put("field1", "value1");
        hashValue.put("field2", "value2");
        event.setValue(hashValue);

        assertTrue(event.isHash());
        assertEquals(hashValue, event.asHash());
        assertEquals(KeyType.hash, event.getType());

        // Test that raw value is still accessible
        assertEquals(hashValue, event.getValue());

        // Test that original properties are preserved
        assertEquals("test:key", event.getKey());
        assertEquals(KeyType.hash, event.getType());
    }

    @Test
    void testStringEvent() {
        KeyStructEvent<String, String> event = new KeyStructEvent<>();
        event.setKey("string:key");
        event.setType(KeyType.string);
        event.setValue("hello world");

        assertTrue(event.isString());
        assertEquals("hello world", event.asString());
        assertEquals(KeyType.string, event.getType());
    }

    @Test
    void testDirectMethods() {
        KeyStructEvent<String, String> event = new KeyStructEvent<>();
        event.setType(KeyType.string);
        event.setValue("test");

        // Test direct type checking and conversion methods
        assertTrue(event.isString());
        assertFalse(event.isHash());
        assertEquals("test", event.asString());

        // Setting a new value should work correctly
        event.setValue("new value");
        assertEquals("new value", event.asString());
    }

    @Test
    void testTypeConversionError() {
        KeyStructEvent<String, String> event = new KeyStructEvent<>();
        event.setKey("string:key");
        event.setType(KeyType.string);
        event.setValue("hello");

        // Should throw exception when trying to convert to wrong type
        assertThrows(IllegalStateException.class, () -> event.asHash());
    }

    @Test
    void testNullValue() {
        KeyStructEvent<String, String> event = new KeyStructEvent<>();
        event.setKey("null:key");
        event.setType(KeyType.none);
        event.setValue(null);

        assertTrue(event.isNone());
        assertNull(event.asString());
    }

}
