package com.redis.riotx.parquet;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

/**
 * Read Avro records from a Parquet file.
 *
 */
public class RiotParquetReader extends ParquetReader<Map<String, Object>> {

	public RiotParquetReader(Path file, ReadSupport<Map<String, Object>> readSupport) throws IOException {
		super(file, readSupport);
	}

	public static <T> Builder builder(InputFile file) {
		return new Builder(file);
	}

	public static class Builder extends ParquetReader.Builder<Map<String, Object>> {

		private Builder(InputFile file) {
			super(file);
		}

		protected ReadSupport<Map<String, Object>> getReadSupport() {
			return new MapReadSupport();
		}
	}
}
