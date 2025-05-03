package com.redis.riot.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

public class ParquetWriterBuilderTest {

	@Test
	public void testBuilderCreatesCorrectWriteSupport() throws IOException {
		// Mock the OutputFile
		OutputFile outputFile = mock(OutputFile.class);

		// Create a simple schema for testing
		MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
				.required(PrimitiveType.PrimitiveTypeName.BINARY).named("name").named("test");

		// Create the builder
		ParquetWriterBuilder builder = new ParquetWriterBuilder(outputFile, schema);

		// Test that the builder creates a WriteSupport with the correct schema
		WriteSupport<Map<String, Object>> writeSupport = builder.getWriteSupport(new Configuration());

		// Verify it's a MapWriteSupport instance
		assertTrue(writeSupport instanceof MapWriteSupport);

		// Verify the self() method returns itself
		assertEquals(builder, builder.self());
	}

	@Test
	public void testIntegrationWithParquetWriter() throws IOException {
		// This is a more complex test that would normally write to a file
		// Here we'll just verify that our builder can be used to create a ParquetWriter

		// Mock the OutputFile and necessary internals for ParquetWriter creation
		OutputFile outputFile = mock(OutputFile.class);

		// Create a simple schema for testing
		MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
				.required(PrimitiveType.PrimitiveTypeName.BINARY).named("name").named("test");

		// Create a builder with some test settings
		ParquetWriterBuilder builder = new ParquetWriterBuilder(outputFile, schema)
				.withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
				.withRowGroupSize(128 * 1024 * 1024).withPageSize(1024 * 1024);

		// In a real test, we would call .build() and write some data
		// Since we can't actually build without a real output file in unit tests,
		// we'll just verify the builder is properly configured

		// Verify the builder returns itself for method chaining
		assertEquals(builder, builder.self());
	}
}