package com.redis.riot.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ParquetIntegrationTest {

    @TempDir
    Path tempDir;

    private Path parquetFile;

    private Configuration hadoopConfig;

    private MessageType schema;

    private Logger log = LoggerFactory.getLogger(getClass());

    @BeforeEach
    void setUp() throws IOException {
        parquetFile = tempDir.resolve("test-data.parquet");
        hadoopConfig = new Configuration();

        // Define a schema with supported Parquet data types (INT96 removed)
        schema = MessageTypeParser.parseMessageType(
                "message test { " + "  required binary string_field (UTF8); " + "  required int32 int32_field; "
                        + "  optional int64 int64_field; " + "  optional float float_field; "
                        + "  optional double double_field; " + "  optional boolean boolean_field; "
                        + "  optional binary binary_field; " + "  optional fixed_len_byte_array(16) fixed_binary_field; "
                        + "}");
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(parquetFile);
    }

    @Test
    void testAllParquetDataTypes() throws IOException {
        // Prepare test data with all Parquet data types
        List<Map<String, Object>> testRecords = createTestRecordsWithAllTypes();

        // Write test data
        writeParquetFile(testRecords);

        // Verify file was created
        assertTrue(Files.exists(parquetFile), "Parquet file should exist after writing");
        assertTrue(Files.size(parquetFile) > 0, "Parquet file should not be empty");

        // Read and verify data
        List<Map<String, Object>> readRecords = readParquetFile();

        // Verify record count
        assertEquals(testRecords.size(), readRecords.size(), "Should read same number of records as written");

        // Verify record contents - first record as example
        Map<String, Object> expected = testRecords.get(0);
        Map<String, Object> actual = readRecords.get(0);

        // String field
        assertEquals(expected.get("string_field"), actual.get("string_field"));

        // Numeric fields
        assertEquals(expected.get("int32_field"), actual.get("int32_field"));
        assertEquals(expected.get("int64_field"), actual.get("int64_field"));

        // Floating point fields
        float expectedFloat = (Float) expected.get("float_field");
        float actualFloat = ((Number) actual.get("float_field")).floatValue();
        assertEquals(expectedFloat, actualFloat, 0.0001);

        double expectedDouble = (Double) expected.get("double_field");
        double actualDouble = ((Number) actual.get("double_field")).doubleValue();
        assertEquals(expectedDouble, actualDouble, 0.0001);

        // Boolean field
        assertEquals(expected.get("boolean_field"), actual.get("boolean_field"));

        // Binary fields
        byte[] expectedBinary = (byte[]) expected.get("binary_field");
        byte[] actualBinary = (byte[]) actual.get("binary_field");
        assertArrayEquals(expectedBinary, actualBinary);

        // Fixed length binary
        byte[] expectedFixedBinary = (byte[]) expected.get("fixed_binary_field");
        byte[] actualFixedBinary = (byte[]) actual.get("fixed_binary_field");
        assertArrayEquals(expectedFixedBinary, actualFixedBinary);
    }

    @Test
    void testBinaryFieldsSpecifically() throws IOException {
        // Create test records with various binary values
        List<Map<String, Object>> binaryRecords = new ArrayList<>();

        // Record with empty binary
        Map<String, Object> emptyRecord = createBaseRecord();
        emptyRecord.put("binary_field", new byte[0]);
        emptyRecord.put("fixed_binary_field", new byte[16]); // zeros
        binaryRecords.add(emptyRecord);

        // Record with small binary
        Map<String, Object> smallRecord = createBaseRecord();
        smallRecord.put("string_field", "Small binary");
        smallRecord.put("int32_field", 2);
        smallRecord.put("binary_field", "Hello".getBytes(StandardCharsets.UTF_8));
        byte[] fixedSmall = new byte[16];
        Arrays.fill(fixedSmall, (byte) 1);
        smallRecord.put("fixed_binary_field", fixedSmall);
        binaryRecords.add(smallRecord);

        // Record with larger binary
        Map<String, Object> largeRecord = createBaseRecord();
        largeRecord.put("string_field", "Large binary");
        largeRecord.put("int32_field", 3);
        largeRecord.put("binary_field", new byte[8192]); // 8KB
        Arrays.fill((byte[]) largeRecord.get("binary_field"), (byte) 0xFF);
        byte[] fixedLarge = new byte[16];
        Arrays.fill(fixedLarge, (byte) 0xFF);
        largeRecord.put("fixed_binary_field", fixedLarge);
        binaryRecords.add(largeRecord);

        // Write and read back
        writeParquetFile(binaryRecords);
        List<Map<String, Object>> readRecords = readParquetFile();

        // Verify empty binary
        byte[] emptyBinary = (byte[]) readRecords.get(0).get("binary_field");
        assertEquals(0, emptyBinary.length);

        byte[] emptyFixed = (byte[]) readRecords.get(0).get("fixed_binary_field");
        assertEquals(16, emptyFixed.length);
        for (byte b : emptyFixed) {
            assertEquals(0, b);
        }

        // Verify small binary
        byte[] smallBinary = (byte[]) readRecords.get(1).get("binary_field");
        assertArrayEquals("Hello".getBytes(StandardCharsets.UTF_8), smallBinary);

        byte[] smallFixed = (byte[]) readRecords.get(1).get("fixed_binary_field");
        for (byte b : smallFixed) {
            assertEquals(1, b);
        }

        // Verify large binary
        byte[] largeBinary = (byte[]) readRecords.get(2).get("binary_field");
        assertEquals(8192, largeBinary.length);
        for (byte b : largeBinary) {
            assertEquals((byte) 0xFF, b);
        }

        byte[] largeFixed = (byte[]) readRecords.get(2).get("fixed_binary_field");
        for (byte b : largeFixed) {
            assertEquals((byte) 0xFF, b);
        }
    }

    @Test
    void testNumericRanges() throws IOException {
        List<Map<String, Object>> numericRecords = new ArrayList<>();

        // Add min/max values for each numeric type
        Map<String, Object> minValues = createBaseRecord();
        minValues.put("string_field", "Min values");
        minValues.put("int32_field", Integer.MIN_VALUE);
        minValues.put("int64_field", Long.MIN_VALUE);
        minValues.put("float_field", Float.MIN_VALUE);
        minValues.put("double_field", Double.MIN_VALUE);
        numericRecords.add(minValues);

        Map<String, Object> maxValues = createBaseRecord();
        maxValues.put("string_field", "Max values");
        maxValues.put("int32_field", Integer.MAX_VALUE);
        maxValues.put("int64_field", Long.MAX_VALUE);
        maxValues.put("float_field", Float.MAX_VALUE);
        maxValues.put("double_field", Double.MAX_VALUE);
        numericRecords.add(maxValues);

        // Add special values
        Map<String, Object> specialValues = createBaseRecord();
        specialValues.put("string_field", "Special values");
        specialValues.put("int32_field", 0);
        specialValues.put("int64_field", 0L);
        specialValues.put("float_field", Float.NaN);
        specialValues.put("double_field", Double.POSITIVE_INFINITY);
        numericRecords.add(specialValues);

        // Write and read back
        writeParquetFile(numericRecords);
        List<Map<String, Object>> readRecords = readParquetFile();

        // Verify min values
        assertEquals(Integer.MIN_VALUE, readRecords.get(0).get("int32_field"));
        assertEquals(Long.MIN_VALUE, readRecords.get(0).get("int64_field"));

        // MIN_VALUE for float/double are very small positive numbers, not negative
        // infinity
        float minFloat = ((Number) readRecords.get(0).get("float_field")).floatValue();
        assertTrue(minFloat > 0 && minFloat < 1e-30f);

        double minDouble = ((Number) readRecords.get(0).get("double_field")).doubleValue();
        assertTrue(minDouble > 0 && minDouble < 1e-300);

        // Verify max values
        assertEquals(Integer.MAX_VALUE, readRecords.get(1).get("int32_field"));
        assertEquals(Long.MAX_VALUE, readRecords.get(1).get("int64_field"));

        float maxFloat = ((Number) readRecords.get(1).get("float_field")).floatValue();
        assertTrue(maxFloat > 1e30f);

        double maxDouble = ((Number) readRecords.get(1).get("double_field")).doubleValue();
        assertTrue(maxDouble > 1e300);

        // Verify special values (NaN, Infinity)
        float nanFloat = ((Number) readRecords.get(2).get("float_field")).floatValue();
        assertTrue(Float.isNaN(nanFloat));

        double infDouble = ((Number) readRecords.get(2).get("double_field")).doubleValue();
        assertTrue(Double.isInfinite(infDouble) && infDouble > 0);
    }

    // TODO: Fix LIST support - currently only reads first element of arrays
    @Disabled
    @Test
    void testListSupport() throws IOException {
        // Define a schema with a LIST of floats (like vector embeddings)
        MessageType listSchema = MessageTypeParser.parseMessageType(
                "message test { " + "  required int64 id; " + "  optional group vector (LIST) { " + "    repeated group list { "
                        + "      optional float element; " + "    } " + "  } " + "}");

        Path listParquetFile = tempDir.resolve("list-data.parquet");

        // Create test data with float arrays (like 64-dimensional vectors)
        List<Map<String, Object>> listRecords = new ArrayList<>();

        // Record 1: Vector with 64 floats
        Map<String, Object> record1 = new HashMap<>();
        record1.put("id", 1L);
        float[] vector1 = new float[64];
        for (int i = 0; i < 64; i++) {
            vector1[i] = i * 0.1f; // 0.0, 0.1, 0.2, ..., 6.3
        }
        record1.put("vector", vector1);
        listRecords.add(record1);

        // Record 2: Vector with different 64 floats
        Map<String, Object> record2 = new HashMap<>();
        record2.put("id", 2L);
        float[] vector2 = new float[64];
        for (int i = 0; i < 64; i++) {
            vector2[i] = (float) Math.sin(i * Math.PI / 32); // sine wave pattern
        }
        record2.put("vector", vector2);
        listRecords.add(record2);

        // Record 3: Smaller vector (to test variable-length lists)
        Map<String, Object> record3 = new HashMap<>();
        record3.put("id", 3L);
        float[] vector3 = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
        record3.put("vector", vector3);
        listRecords.add(record3);

        // Write test data using the list schema
        writeParquetFileWithSchema(listRecords, listSchema, listParquetFile);

        // Read and verify data
        List<Map<String, Object>> readRecords = readParquetFileFromPath(listParquetFile);

        // Verify record count
        assertEquals(listRecords.size(), readRecords.size(), "Should read same number of records as written");

        // Verify first record (64-element vector)
        Map<String, Object> readRecord1 = readRecords.get(0);
        assertEquals(1L, readRecord1.get("id"));
        Object[] readVector1 = (Object[]) readRecord1.get("vector");
        assertEquals(64, readVector1.length, "Vector should have 64 elements");

        // Check first few and last few elements
        assertEquals(0.0f, ((Number) readVector1[0]).floatValue(), 0.001f);
        assertEquals(0.1f, ((Number) readVector1[1]).floatValue(), 0.001f);
        assertEquals(6.3f, ((Number) readVector1[63]).floatValue(), 0.001f);

        // Verify second record (sine wave pattern)
        Map<String, Object> readRecord2 = readRecords.get(1);
        assertEquals(2L, readRecord2.get("id"));
        Object[] readVector2 = (Object[]) readRecord2.get("vector");
        assertEquals(64, readVector2.length, "Vector should have 64 elements");

        // Check sine wave values
        assertEquals(0.0f, ((Number) readVector2[0]).floatValue(), 0.001f); // sin(0)
        assertEquals(1.0f, ((Number) readVector2[16]).floatValue(), 0.001f); // sin(π/2)
        assertEquals(0.0f, ((Number) readVector2[32]).floatValue(), 0.001f); // sin(π)

        // Verify third record (smaller vector)
        Map<String, Object> readRecord3 = readRecords.get(2);
        assertEquals(3L, readRecord3.get("id"));
        Object[] readVector3 = (Object[]) readRecord3.get("vector");
        assertEquals(5, readVector3.length, "Vector should have 5 elements");

        for (int i = 0; i < 5; i++) {
            assertEquals((float) (i + 1), ((Number) readVector3[i]).floatValue(), 0.001f);
        }
    }

    @Test
    void testStringValues() throws IOException {
        List<Map<String, Object>> stringRecords = new ArrayList<>();

        // Empty string
        Map<String, Object> emptyString = createBaseRecord();
        emptyString.put("string_field", "");
        emptyString.put("int32_field", 1);
        stringRecords.add(emptyString);

        // String with special characters
        Map<String, Object> specialString = createBaseRecord();
        specialString.put("string_field", "Special: \t\n\r\u0080\u2022\u3000\uD83D\uDE00");
        specialString.put("int32_field", 2);
        stringRecords.add(specialString);

        // Very long string
        StringBuilder longStringBuilder = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            longStringBuilder.append("abcdefghij");
        }
        Map<String, Object> longString = createBaseRecord();
        longString.put("string_field", longStringBuilder.toString());
        longString.put("int32_field", 3);
        stringRecords.add(longString);

        // Write and read back
        writeParquetFile(stringRecords);
        List<Map<String, Object>> readRecords = readParquetFile();

        // Verify empty string
        assertEquals("", readRecords.get(0).get("string_field"));

        // Verify special characters
        assertEquals("Special: \t\n\r\u0080\u2022\u3000\uD83D\uDE00", readRecords.get(1).get("string_field"));

        // Verify long string
        String readLongString = (String) readRecords.get(2).get("string_field");
        assertEquals(100000, readLongString.length());
        assertEquals(longStringBuilder.toString(), readLongString);
    }

    private Map<String, Object> createBaseRecord() {
        Map<String, Object> record = new HashMap<>();
        record.put("string_field", "Base record");
        record.put("int32_field", 1);
        record.put("int64_field", 1234567890L);
        record.put("float_field", 1.5f);
        record.put("double_field", 3.14159);
        record.put("boolean_field", true);
        record.put("binary_field", new byte[] { 1, 2, 3, 4 });
        record.put("fixed_binary_field", new byte[16]); // 16 bytes for fixed binary
        return record;
    }

    private List<Map<String, Object>> createTestRecordsWithAllTypes() {
        List<Map<String, Object>> records = new ArrayList<>();

        // Create first record with all types
        Map<String, Object> record1 = createBaseRecord();
        record1.put("string_field", "Record with all types");
        records.add(record1);

        // Create second record with different values
        Map<String, Object> record2 = createBaseRecord();
        record2.put("string_field", "Another record with all types");
        record2.put("int32_field", -42);
        record2.put("int64_field", -9876543210L);
        record2.put("float_field", -2.5f);
        record2.put("double_field", -2.71828);
        record2.put("boolean_field", false);

        // Different binary data
        byte[] binary2 = "Different binary data".getBytes(StandardCharsets.UTF_8);
        record2.put("binary_field", binary2);

        byte[] fixed2 = new byte[16];
        Arrays.fill(fixed2, (byte) 0xAA);
        record2.put("fixed_binary_field", fixed2);

        records.add(record2);

        return records;
    }

    private void writeParquetFile(List<Map<String, Object>> records) throws IOException {
        OutputFile outputFile = HadoopOutputFile.fromPath(
                new org.apache.hadoop.fs.Path(parquetFile.toAbsolutePath().toString()), hadoopConfig);

        try (ParquetWriter<Map<String, Object>> writer = new ParquetWriterBuilder(outputFile, schema).build()) {
            for (Map<String, Object> record : records) {
                writer.write(record);
            }
        }
        log.info("Wrote file {}", parquetFile.toAbsolutePath());
    }

    private List<Map<String, Object>> readParquetFile() throws IOException {
        return readParquetFileFromPath(parquetFile);
    }

    private List<Map<String, Object>> readParquetFileFromPath(Path filePath) throws IOException {
        InputFile inputFile = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(filePath.toAbsolutePath().toString()),
                hadoopConfig);

        List<Map<String, Object>> records = new ArrayList<>();

        try (ParquetReader<Map<String, Object>> reader = new ParquetReaderBuilder(inputFile).build()) {
            Map<String, Object> record;
            while ((record = reader.read()) != null) {
                records.add(new HashMap<>(record));
            }
        }

        return records;
    }

    private void writeParquetFileWithSchema(List<Map<String, Object>> records, MessageType schema, Path filePath)
            throws IOException {
        OutputFile outputFile = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(filePath.toAbsolutePath().toString()),
                hadoopConfig);

        try (ParquetWriter<Map<String, Object>> writer = new ParquetWriterBuilder(outputFile, schema).build()) {
            for (Map<String, Object> record : records) {
                writer.write(record);
            }
        }
        log.info("Wrote file {}", filePath.toAbsolutePath());
    }

}
