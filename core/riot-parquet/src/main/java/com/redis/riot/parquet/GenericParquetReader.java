package com.redis.riot.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic Parquet reader that can handle any Parquet file structure Includes configuration to handle PyArrow's legacy list
 * serialization format
 */
public class GenericParquetReader implements AutoCloseable {

    private final InputFile file;

    private final ParquetReader<GenericRecord> reader;

    private GenericRecord currentRecord;

    private boolean hasNextRecord = true;

    private Schema schema;

    private List<String> columnNames;

    private Map<String, String> columnTypes;

    /**
     * Create reader from file path
     */
    public GenericParquetReader(InputFile file, Configuration conf) throws IOException {
        this.file = file;
        this.reader = AvroParquetReader.<GenericRecord> builder(file).withConf(conf).build();
        initialize();
    }

    private void initialize() throws IOException {
        // Read first record to get schema information
        this.currentRecord = reader.read();
        this.hasNextRecord = (currentRecord != null);

        if (currentRecord != null) {
            this.schema = currentRecord.getSchema();
            this.columnNames = new ArrayList<>();
            this.columnTypes = new HashMap<>();

            // Extract column information
            for (Schema.Field field : schema.getFields()) {
                String fieldName = field.name();
                String fieldType = getFieldTypeDescription(field.schema());

                columnNames.add(fieldName);
                columnTypes.put(fieldName, fieldType);
            }
        }
    }

    /**
     * Get schema information as a readable string
     */
    public String getSchemaInfo() {
        if (schema == null) {
            return "No data in file";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Schema Information:\n");
        sb.append("=================\n");

        for (String columnName : columnNames) {
            sb.append(String.format("%-20s : %s\n", columnName, columnTypes.get(columnName)));
        }

        return sb.toString();
    }

    /**
     * Get list of column names
     */
    public List<String> getColumnNames() {
        return new ArrayList<>(columnNames);
    }

    /**
     * Get column type information
     */
    public Map<String, String> getColumnTypes() {
        return new HashMap<>(columnTypes);
    }

    /**
     * Check if there are more records
     */
    public boolean hasNext() {
        return hasNextRecord;
    }

    /**
     * Get the next record as a Map
     */
    public Map<String, Object> nextRecord() throws IOException {
        if (!hasNextRecord) {
            return null;
        }

        Map<String, Object> recordMap = new HashMap<>();

        // Convert current record to map
        for (String columnName : columnNames) {
            Object value = currentRecord.get(columnName);
            recordMap.put(columnName, convertValue(value));
        }

        // Read next record
        currentRecord = reader.read();
        hasNextRecord = (currentRecord != null);

        return recordMap;
    }

    /**
     * Get the next record as GenericRecord (for advanced usage)
     */
    public GenericRecord nextGenericRecord() throws IOException {
        if (!hasNextRecord) {
            return null;
        }

        GenericRecord record = currentRecord;

        // Read next record
        currentRecord = reader.read();
        hasNextRecord = (currentRecord != null);

        return record;
    }

    /**
     * Read all records into a list (careful with memory for large files)
     */
    public List<Map<String, Object>> readAll() throws IOException {
        List<Map<String, Object>> records = new ArrayList<>();

        while (hasNext()) {
            records.add(nextRecord());
        }

        return records;
    }

    /**
     * Print first N records for inspection
     */
    public void printSample(int maxRecords) throws IOException {
        System.out.println(getSchemaInfo());
        System.out.println("\nSample Records:");
        System.out.println("===============");

        int count = 0;
        while (hasNext() && count < maxRecords) {
            Map<String, Object> record = nextRecord();
            System.out.println("Record " + (count + 1) + ":");

            for (Map.Entry<String, Object> entry : record.entrySet()) {
                System.out.println("  " + entry.getKey() + ": " + formatValue(entry.getValue()));
            }
            System.out.println();
            count++;
        }
    }

    private String getFieldTypeDescription(Schema fieldSchema) {
        Schema.Type type = fieldSchema.getType();

        switch (type) {
            case UNION:
                // Handle nullable fields (common in Parquet)
                List<Schema> types = fieldSchema.getTypes();
                List<String> typeNames = new ArrayList<>();
                for (Schema unionType : types) {
                    if (unionType.getType() != Schema.Type.NULL) {
                        typeNames.add(getFieldTypeDescription(unionType));
                    }
                }
                return String.join(" | ", typeNames) + " (nullable)";

            case ARRAY:
                return "Array<" + getFieldTypeDescription(fieldSchema.getElementType()) + ">";

            case MAP:
                return "Map<String, " + getFieldTypeDescription(fieldSchema.getValueType()) + ">";

            case RECORD:
                return "Record (" + fieldSchema.getName() + ")";

            default:
                return type.toString().toLowerCase();
        }
    }

    private Object convertValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof GenericArray) {
            // Convert GenericArray to List
            GenericArray<?> array = (GenericArray<?>) value;
            List<Object> list = new ArrayList<>();
            for (Object item : array) {
                list.add(convertValue(item)); // Recursive for nested structures
            }
            return list;
        }

        if (value instanceof GenericRecord) {
            // Convert nested records to Maps
            GenericRecord record = (GenericRecord) value;
            Map<String, Object> map = new HashMap<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                map.put(field.name(), convertValue(record.get(field.name())));
            }
            return map;
        }

        if (value instanceof Map) {
            // Convert nested maps
            Map<?, ?> originalMap = (Map<?, ?>) value;
            Map<String, Object> convertedMap = new HashMap<>();
            for (Map.Entry<?, ?> entry : originalMap.entrySet()) {
                convertedMap.put(entry.getKey().toString(), convertValue(entry.getValue()));
            }
            return convertedMap;
        }

        if (value instanceof ByteBuffer) {
            return ((ByteBuffer) value).array();
        }
        // For primitive types, return as-is
        return value;
    }

    private String formatValue(Object value) {
        if (value == null) {
            return "null";
        }

        if (value instanceof List) {
            List<?> list = (List<?>) value;
            if (list.size() > 5) {
                return "[" + list.subList(0, 5) + "... (" + list.size() + " items)]";
            }
            return list.toString();
        }

        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            if (map.size() > 3) {
                return "{" + map.size() + " fields}";
            }
            return map.toString();
        }

        String str = value.toString();
        if (str.length() > 100) {
            return str.substring(0, 100) + "...";
        }

        return str;
    }

    @Override
    public void close() throws Exception {
        if (reader != null) {
            reader.close();
        }
        if (file instanceof AutoCloseable) {
            ((AutoCloseable) file).close();
        }
    }

}
