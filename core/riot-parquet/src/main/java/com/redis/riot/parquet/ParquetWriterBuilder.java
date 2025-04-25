package com.redis.riot.parquet;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetWriterBuilder extends ParquetWriter.Builder<Map<String, Object>, ParquetWriterBuilder> {

	private final MessageType schema;

	public ParquetWriterBuilder(OutputFile file, MessageType schema) {
		super(file);
		this.schema = schema;
	}

	@Override
	protected WriteSupport<Map<String, Object>> getWriteSupport(Configuration conf) {
		return new MapWriteSupport(schema);
	}

	@Override
	protected ParquetWriterBuilder self() {
		return this;
	}
}