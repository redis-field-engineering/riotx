package com.redis.riotx.parquet;

import java.util.Map;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

public class ParquetReaderBuilder extends ParquetReader.Builder<Map<String, Object>> {

	public ParquetReaderBuilder(InputFile file) {
		super(file);
	}

	@Override
	public ReadSupport<Map<String, Object>> getReadSupport() {
		return new MapReadSupport();
	}

}