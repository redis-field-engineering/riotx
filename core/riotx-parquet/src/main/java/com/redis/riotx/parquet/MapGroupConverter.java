package com.redis.riotx.parquet;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

class MapGroupConverter extends GroupConverter {

	protected Map<String, Object> current;
	private Converter[] converters;

	MapGroupConverter(MapGroupConverter parent, GroupType schema) {
		converters = new Converter[schema.getFieldCount()];

		for (int i = 0; i < converters.length; i++) {
			final String field = schema.getFieldName(i);
			final Type type = schema.getType(i);
			if (type.isPrimitive()) {
				converters[i] = new MapPrimitiveConverter(this, field, type);
			} else {
				converters[i] = new MapGroupConverter(this, type.asGroupType());
			}
		}
	}

	@Override
	public void start() {
		current = new LinkedHashMap<>();
	}

	@Override
	public Converter getConverter(int fieldIndex) {
		return converters[fieldIndex];
	}

	@Override
	public void end() {
	}

	public Map<String, Object> getCurrentRecord() {
		return current;
	}
}
