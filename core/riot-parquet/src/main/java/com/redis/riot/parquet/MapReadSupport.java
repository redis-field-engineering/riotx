package com.redis.riot.parquet;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class MapReadSupport extends ReadSupport<Map<String, Object>> {

	@Override
	public ReadContext init(Configuration configuration, Map<String, String> metadata, MessageType schema) {
		String partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA);
		MessageType requestedProjection = getSchemaForRead(schema, partialSchemaString);
		return new ReadContext(requestedProjection);
	}

	@Override
	public RecordMaterializer<Map<String, Object>> prepareForRead(Configuration configuration,
			Map<String, String> metadata, MessageType schema, ReadContext context) {
		return new MapRecordConverter(context.getRequestedSchema());
	}

}
