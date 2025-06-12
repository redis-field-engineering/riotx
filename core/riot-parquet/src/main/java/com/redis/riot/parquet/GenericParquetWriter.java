package com.redis.riot.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic Parquet writer that can write any data structure to Parquet files Compatible with PyArrow and other readers
 */
public class GenericParquetWriter implements AutoCloseable {

    private final OutputFile outputFile;

    private final ParquetWriter<GenericRecord> writer;

    private final Schema schema;

    /**
     * Create writer for file path
     */
    public GenericParquetWriter(OutputFile outputFile, Schema schema, Configuration conf, CompressionCodecName compression,
            long blockSize, int pageSize, boolean enableDictionary, boolean enableValidation) throws IOException {
        this.schema = schema;
        this.outputFile = outputFile;

        this.writer = AvroParquetWriter.<GenericRecord> builder(outputFile).withSchema(schema).withConf(conf)
                .withCompressionCodec(compression).withRowGroupSize(blockSize).withPageSize(pageSize)
                .withDictionaryEncoding(enableDictionary).withValidation(enableValidation).build();
    }

    /**
     * Write a record from a Map
     */
    public void writeRecord(Map<String, Object> recordMap) throws IOException {
        GenericRecord record = createRecordFromMap(recordMap);
        writer.write(record);
    }

    /**
     * Write multiple records from Maps
     */
    public void writeRecords(List<Map<String, Object>> records) throws IOException {
        for (Map<String, Object> record : records) {
            writeRecord(record);
        }
    }

    /**
     * Write a GenericRecord directly
     */
    public void writeGenericRecord(GenericRecord record) throws IOException {
        writer.write(record);
    }

    /**
     * Write multiple GenericRecords
     */
    public void writeGenericRecords(List<GenericRecord> records) throws IOException {
        for (GenericRecord record : records) {
            writeGenericRecord(record);
        }
    }

    /**
     * Create a GenericRecord from a Map based on the schema
     */
    private GenericRecord createRecordFromMap(Map<String, Object> recordMap) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);

        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Object value = recordMap.get(fieldName);

            if (value != null) {
                Object convertedValue = convertValueForAvro(value, field.schema());
                builder.set(fieldName, convertedValue);
            }
        }

        return builder.build();
    }

    /**
     * Convert Java objects to Avro-compatible types
     */
    private Object convertValueForAvro(Object value, Schema fieldSchema) {
        if (value == null) {
            return null;
        }

        Schema actualSchema = fieldSchema;

        // Handle union types (nullable fields)
        if (fieldSchema.getType() == Schema.Type.UNION) {
            // Find the non-null type in the union
            for (Schema unionType : fieldSchema.getTypes()) {
                if (unionType.getType() != Schema.Type.NULL) {
                    actualSchema = unionType;
                    break;
                }
            }
        }

        switch (actualSchema.getType()) {
            case ARRAY:
                if (value instanceof List) {
                    List<?> list = (List<?>) value;
                    GenericData.Array<Object> array = new GenericData.Array<>(actualSchema, new ArrayList<>());
                    for (Object item : list) {
                        Object convertedItem = convertValueForAvro(item, actualSchema.getElementType());
                        array.add(convertedItem);
                    }
                    return array;
                } else if (value.getClass().isArray()) {
                    // Handle primitive arrays
                    Object[] objectArray = (Object[]) value;
                    GenericData.Array<Object> array = new GenericData.Array<>(actualSchema, new ArrayList<>());
                    for (Object item : objectArray) {
                        Object convertedItem = convertValueForAvro(item, actualSchema.getElementType());
                        array.add(convertedItem);
                    }
                    return array;
                }
                break;

            case MAP:
                if (value instanceof Map) {
                    Map<?, ?> map = (Map<?, ?>) value;
                    Map<String, Object> convertedMap = new HashMap<>();
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        String key = entry.getKey().toString();
                        Object convertedValue = convertValueForAvro(entry.getValue(), actualSchema.getValueType());
                        convertedMap.put(key, convertedValue);
                    }
                    return convertedMap;
                }
                break;

            case RECORD:
                if (value instanceof Map) {
                    Map<?, ?> map = (Map<?, ?>) value;
                    GenericRecordBuilder builder = new GenericRecordBuilder(actualSchema);
                    for (Schema.Field field : actualSchema.getFields()) {
                        Object fieldValue = map.get(field.name());
                        if (fieldValue != null) {
                            Object convertedValue = convertValueForAvro(fieldValue, field.schema());
                            builder.set(field.name(), convertedValue);
                        }
                    }
                    return builder.build();
                }
                break;

            case STRING:
                return value.toString();

            case BYTES:
                if (value instanceof byte[]) {
                    return java.nio.ByteBuffer.wrap((byte[]) value);
                } else if (value instanceof String) {
                    return java.nio.ByteBuffer.wrap(((String) value).getBytes());
                }
                break;

            case INT:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                break;

            case LONG:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                break;

            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                break;

            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                break;

            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                } else {
                    return Boolean.parseBoolean(value.toString());
                }

            default:
                return value;
        }

        return value;
    }

    /**
     * Close the writer and copy to OutputStream if needed
     */
    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }

        // If we used a temporary file for OutputStream, copy it now
        if (outputFile instanceof AutoCloseable) {
            ((AutoCloseable) outputFile).close();
        }
    }

    /**
     * Get the schema being used
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Utility method to create a schema from field definitions
     */
    public static Schema createSchema(String recordName, Map<String, FieldType> fields) {
        SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(recordName);
        SchemaBuilder.FieldAssembler<Schema> assembler = builder.fields();

        for (Map.Entry<String, FieldType> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            FieldType fieldType = entry.getValue();

            switch (fieldType) {
                case LONG:
                    assembler = assembler.optionalLong(fieldName);
                    break;
                case INT:
                    assembler = assembler.optionalInt(fieldName);
                    break;
                case FLOAT:
                    assembler = assembler.optionalFloat(fieldName);
                    break;
                case BINARY:
                    assembler = assembler.optionalBytes(fieldName);
                    break;
                case DOUBLE:
                    assembler = assembler.optionalDouble(fieldName);
                    break;
                case STRING:
                    assembler = assembler.optionalString(fieldName);
                    break;
                case BOOLEAN:
                    assembler = assembler.optionalBoolean(fieldName);
                    break;
                case FLOAT_ARRAY:
                    assembler = assembler.name(fieldName).type().optional().array().items().floatType();
                    break;
                case DOUBLE_ARRAY:
                    assembler = assembler.name(fieldName).type().optional().array().items().doubleType();
                    break;
                case STRING_ARRAY:
                    assembler = assembler.name(fieldName).type().optional().array().items().stringType();
                    break;
            }
        }

        return assembler.endRecord();
    }

    /**
     * Enum for common field types
     */
    public enum FieldType {
        LONG, INT, FLOAT, DOUBLE, STRING, BINARY, BOOLEAN, FLOAT_ARRAY, DOUBLE_ARRAY, STRING_ARRAY
    }

}
