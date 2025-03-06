package com.redis.riotx.parquet;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Map;

import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.Test;

public class ParquetReaderBuilderTest {

	@Test
	public void testBuilderCreatesCorrectReadSupport() throws IOException {
		// Mock the InputFile
		InputFile inputFile = mock(InputFile.class);

		// Create the builder
		ParquetReaderBuilder builder = new ParquetReaderBuilder(inputFile);

		// Test that the builder creates a ReadSupport with the correct type
		ReadSupport<Map<String, Object>> readSupport = builder.getReadSupport();

		// Verify it's a MapReadSupport instance
		assertTrue(readSupport instanceof MapReadSupport);
	}

}