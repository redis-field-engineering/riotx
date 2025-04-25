package com.redis.riot.parquet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MapReadSupportTest {

    private MapReadSupport readSupport;
    private MessageType schema;
    private Configuration configuration;
    private Map<String, String> metadata;

    @BeforeEach
    public void setUp() {
        readSupport = new MapReadSupport();
        
        // Create a sample schema
        schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("booleanField")
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("intField")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("longField")
                .required(PrimitiveType.PrimitiveTypeName.FLOAT).named("floatField")
                .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("doubleField")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("stringField")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("bytesField")
                .requiredGroup()
                    .required(PrimitiveType.PrimitiveTypeName.INT32).named("nestedInt")
                    .required(PrimitiveType.PrimitiveTypeName.BINARY).named("nestedString")
                    .named("nestedGroupField")
                .named("message");
        
        configuration = new Configuration();
        metadata = new HashMap<>();
    }

    @Test
    public void testInitWithDefaultSchema() {
        ReadContext readContext = readSupport.init(configuration, metadata, schema);
        
        // Without a specific projection, it should use the full schema
        assertEquals(schema, readContext.getRequestedSchema());
    }

    @Test
    public void testInitWithPartialSchema() {
        // Create a projection schema with only a subset of fields
        MessageType projectionSchema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("intField")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("stringField")
                .named("message");
        
        // Set the projection in the configuration
        configuration.set(ReadSupport.PARQUET_READ_SCHEMA, projectionSchema.toString());
        
        ReadContext readContext = readSupport.init(configuration, metadata, schema);
        
        // The resulting requested schema should match our projection
        MessageType requestedSchema = readContext.getRequestedSchema();
        
        // Verify the schema has only the fields we requested
        assertEquals(2, requestedSchema.getFields().size());
        assertTrue(requestedSchema.containsField("intField"));
        assertTrue(requestedSchema.containsField("stringField"));
    }

    @Test
    public void testPrepareForRead() {
        // First, get a read context
        ReadContext readContext = readSupport.init(configuration, metadata, schema);
        
        // Then prepare for read
        RecordMaterializer<Map<String, Object>> materializer = 
            readSupport.prepareForRead(configuration, metadata, schema, readContext);
        
        // Verify we get a non-null materializer of the right type
        assertNotNull(materializer);
        assertTrue(materializer instanceof MapRecordConverter);
        
    }
    
    @Test
    public void testEndToEndReadFlow() {
        // This test simulates the end-to-end flow when reading data
        
        // First initialize
        ReadContext readContext = readSupport.init(configuration, metadata, schema);
        
        // Then prepare for read
        RecordMaterializer<Map<String, Object>> materializer = 
            readSupport.prepareForRead(configuration, metadata, schema, readContext);
        
        // Verify correct types
        assertNotNull(materializer);
        assertTrue(materializer instanceof MapRecordConverter);
    }
}

