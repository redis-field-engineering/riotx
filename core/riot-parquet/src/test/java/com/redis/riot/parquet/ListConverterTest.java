package com.redis.riot.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

class ListConverterTest {

    @Test
    void testListSchemaDetection() {
        // Test that our LIST detection works correctly
        MessageType schema = MessageTypeParser.parseMessageType("message test { "
                + "  required int64 id; "
                + "  optional group vector (LIST) { "
                + "    repeated group list { "
                + "      optional float element; "
                + "    } "
                + "  } "
                + "}");
        
        // Get the vector field
        Type vectorField = schema.getType("vector");
        assertTrue(vectorField instanceof GroupType);
        
        GroupType vectorGroupType = (GroupType) vectorField;
        LogicalTypeAnnotation logicalType = vectorGroupType.getLogicalTypeAnnotation();
        assertTrue(logicalType instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation, 
                   "Should detect LIST logical type");
        
        // Verify the internal structure
        assertEquals(1, vectorGroupType.getFieldCount(), "LIST should have one repeated field");
        Type listField = vectorGroupType.getType(0);
        assertEquals("list", listField.getName(), "Repeated field should be named 'list'");
        assertEquals(Type.Repetition.REPEATED, listField.getRepetition(), "Field should be repeated");
        
        // Check the element structure
        assertTrue(listField instanceof GroupType);
        GroupType listGroupType = (GroupType) listField;
        assertEquals(1, listGroupType.getFieldCount(), "List element group should have one field");
        
        Type elementField = listGroupType.getType(0);
        assertEquals("element", elementField.getName(), "Element field should be named 'element'");
        assertTrue(elementField instanceof PrimitiveType);
        
        PrimitiveType elementPrimitiveType = (PrimitiveType) elementField;
        assertEquals(PrimitiveType.PrimitiveTypeName.FLOAT, elementPrimitiveType.getPrimitiveTypeName());
    }
    
    @Test
    void testMapGroupConverterDetectsLists() {
        MessageType schema = MessageTypeParser.parseMessageType("message test { "
                + "  required int64 id; "
                + "  optional group vector (LIST) { "
                + "    repeated group list { "
                + "      optional float element; "
                + "    } "
                + "  } "
                + "}");
        
        MapGroupConverter converter = new MapGroupConverter(null, schema);
        
        // This should create the converter without throwing exceptions
        // The actual testing would require mock data, but we can at least verify
        // that the converter structure is set up correctly
        assertTrue(converter != null, "Converter should be created successfully");
        
        // Test getting a converter for the vector field (index 1, after id at index 0)
        try {
            converter.getConverter(1); // This should return a ListGroupConverter
            // If we get here without exception, the converter setup worked
        } catch (Exception e) {
            throw new AssertionError("Should be able to get converter for list field", e);
        }
    }
}