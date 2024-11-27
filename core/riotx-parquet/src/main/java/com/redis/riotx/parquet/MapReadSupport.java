package com.redis.riotx.parquet;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class MapReadSupport extends ReadSupport<Map<String, Object>> {

	@Override
	public ReadSupport.ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData,
			MessageType fileSchema) {
		String partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA);
		MessageType requestedProjection = getSchemaForRead(fileSchema, partialSchemaString);
		return new ReadContext(requestedProjection);
	}

	@Override
	public RecordMaterializer<Map<String, Object>> prepareForRead(Configuration configuration,
			Map<String, String> keyValueMetaData, MessageType fileSchema, ReadSupport.ReadContext readContext) {
		return new MapRecordConverter(readContext.getRequestedSchema());
	}

}
