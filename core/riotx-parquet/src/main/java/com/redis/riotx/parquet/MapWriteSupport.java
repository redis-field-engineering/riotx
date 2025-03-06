package com.redis.riotx.parquet;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Simple WriteSupport for Map<String, Object>.
 */
class MapWriteSupport extends WriteSupport<Map<String, Object>> {

	private final MessageType schema;
	private RecordConsumer consumer;

	public MapWriteSupport(MessageType schema) {
		this.schema = schema;
	}

	@Override
	public WriteContext init(Configuration configuration) {
		return new WriteContext(schema, new HashMap<>());
	}

	@Override
	public void prepareForWrite(RecordConsumer recordConsumer) {
		this.consumer = recordConsumer;
	}

	@Override
	public void write(Map<String, Object> record) {
		consumer.startMessage();
		for (Type field : schema.getFields()) {
			String fieldName = field.getName();
			if (record.containsKey(fieldName)) {
				Object value = record.get(fieldName);
				if (value != null) {
					int fieldIndex = schema.getFieldIndex(fieldName);
					consumer.startField(fieldName, fieldIndex);
					writeValue(value, field);
					consumer.endField(fieldName, fieldIndex);
				}
			}
		}
		consumer.endMessage();
	}

	private void writeValue(Object value, Type field) {
		if (field.isPrimitive()) {
			writePrimitive(value, field.asPrimitiveType());
		} else {
			if (value instanceof Map) {
				@SuppressWarnings("unchecked")
				Map<String, Object> mapValue = (Map<String, Object>) value;
				consumer.startGroup();

				GroupType groupType = field.asGroupType();
				for (Type subField : groupType.getFields()) {
					String subFieldName = subField.getName();
					if (mapValue.containsKey(subFieldName)) {
						Object subValue = mapValue.get(subFieldName);
						if (subValue != null) {
							int subFieldIndex = groupType.getFieldIndex(subFieldName);
							consumer.startField(subFieldName, subFieldIndex);
							writeValue(subValue, subField);
							consumer.endField(subFieldName, subFieldIndex);
						}
					}
				}

				consumer.endGroup();
			}
		}
	}

	private void writePrimitive(Object value, PrimitiveType primitiveType) {
		switch (primitiveType.getPrimitiveTypeName()) {
		case BOOLEAN:
			consumer.addBoolean(value instanceof Boolean ? (Boolean) value : Boolean.parseBoolean(value.toString()));
			break;
		case INT32:
			consumer.addInteger(value instanceof Integer ? (Integer) value : Integer.parseInt(value.toString()));
			break;
		case INT64:
			consumer.addLong(value instanceof Long ? (Long) value : Long.parseLong(value.toString()));
			break;
		case FLOAT:
			consumer.addFloat(value instanceof Float ? (Float) value : Float.parseFloat(value.toString()));
			break;
		case DOUBLE:
			consumer.addDouble(value instanceof Double ? (Double) value : Double.parseDouble(value.toString()));
			break;
		case BINARY:
			if (value instanceof Binary) {
				consumer.addBinary((Binary) value);
			} else if (value instanceof String) {
				consumer.addBinary(Binary.fromString((String) value)); // Ensure UTF-8 encoding
			} else if (value instanceof byte[]) {
				consumer.addBinary(Binary.fromReusedByteArray((byte[]) value)); // Handle raw byte arrays properly
			} else {
				consumer.addBinary(Binary.fromString(value.toString())); // Default case
			}
			break;
		case FIXED_LEN_BYTE_ARRAY:
			int expectedLength = primitiveType.getTypeLength();
			byte[] bytes;
			if (value instanceof byte[]) {
				bytes = (byte[]) value;
			} else {
				bytes = value.toString().getBytes(StandardCharsets.UTF_8);
			}
			if (bytes.length != expectedLength) {
				bytes = Arrays.copyOf(bytes, expectedLength); // Resize (pad/truncate)
			}
			consumer.addBinary(Binary.fromReusedByteArray(bytes));
			break;
		default:
			throw new UnsupportedOperationException("Cannot write " + primitiveType.getPrimitiveTypeName());
		}
	}
}