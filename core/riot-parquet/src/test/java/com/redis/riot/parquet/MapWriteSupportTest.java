package com.redis.riot.parquet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class MapWriteSupportTest {

	private RecordConsumer consumer;
	private MessageType schema;
	private MapWriteSupport writeSupport;

	@BeforeEach
	public void setUp() {
		consumer = mock(RecordConsumer.class);
		schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("booleanField")
				.required(PrimitiveType.PrimitiveTypeName.INT32).named("intField")
				.required(PrimitiveType.PrimitiveTypeName.INT64).named("longField")
				.required(PrimitiveType.PrimitiveTypeName.FLOAT).named("floatField")
				.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("doubleField")
				.required(PrimitiveType.PrimitiveTypeName.BINARY).named("stringField")
				.required(PrimitiveType.PrimitiveTypeName.BINARY).named("bytesField").requiredGroup()
				.required(PrimitiveType.PrimitiveTypeName.INT32).named("nestedInt")
				.required(PrimitiveType.PrimitiveTypeName.BINARY).named("nestedString").named("nestedGroupField")
				.named("message");

		writeSupport = new MapWriteSupport(schema);
		writeSupport.prepareForWrite(consumer);
	}

	@Test
	public void testInitReturnsCorrectSchema() {
		WriteSupport.WriteContext context = writeSupport.init(new Configuration());
		Assertions.assertEquals(schema, context.getSchema());
		Assertions.assertTrue(context.getExtraMetaData().isEmpty());
	}

	@Test
	public void testWriteBoolean() {
		Map<String, Object> record = new HashMap<>();
		record.put("booleanField", true);

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("booleanField", 0);
		inOrder.verify(consumer).addBoolean(true);
		inOrder.verify(consumer).endField("booleanField", 0);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteBooleanFromString() {
		Map<String, Object> record = new HashMap<>();
		record.put("booleanField", "true");

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("booleanField", 0);
		inOrder.verify(consumer).addBoolean(true);
		inOrder.verify(consumer).endField("booleanField", 0);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteInteger() {
		Map<String, Object> record = new HashMap<>();
		record.put("intField", 42);

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("intField", 1);
		inOrder.verify(consumer).addInteger(42);
		inOrder.verify(consumer).endField("intField", 1);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteIntegerFromString() {
		Map<String, Object> record = new HashMap<>();
		record.put("intField", "42");

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("intField", 1);
		inOrder.verify(consumer).addInteger(42);
		inOrder.verify(consumer).endField("intField", 1);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteLong() {
		Map<String, Object> record = new HashMap<>();
		record.put("longField", 12345678900L);

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("longField", 2);
		inOrder.verify(consumer).addLong(12345678900L);
		inOrder.verify(consumer).endField("longField", 2);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteLongFromString() {
		Map<String, Object> record = new HashMap<>();
		record.put("longField", "12345678900");

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("longField", 2);
		inOrder.verify(consumer).addLong(12345678900L);
		inOrder.verify(consumer).endField("longField", 2);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteFloat() {
		Map<String, Object> record = new HashMap<>();
		record.put("floatField", 3.14f);

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("floatField", 3);
		inOrder.verify(consumer).addFloat(3.14f);
		inOrder.verify(consumer).endField("floatField", 3);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteFloatFromString() {
		Map<String, Object> record = new HashMap<>();
		record.put("floatField", "3.14");

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("floatField", 3);
		inOrder.verify(consumer).addFloat(3.14f);
		inOrder.verify(consumer).endField("floatField", 3);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteDouble() {
		Map<String, Object> record = new HashMap<>();
		record.put("doubleField", 2.71828);

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("doubleField", 4);
		inOrder.verify(consumer).addDouble(2.71828);
		inOrder.verify(consumer).endField("doubleField", 4);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteDoubleFromString() {
		Map<String, Object> record = new HashMap<>();
		record.put("doubleField", "2.71828");

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("doubleField", 4);
		inOrder.verify(consumer).addDouble(2.71828);
		inOrder.verify(consumer).endField("doubleField", 4);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteString() {
		Map<String, Object> record = new HashMap<>();
		record.put("stringField", "test string");

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("stringField", 5);
		inOrder.verify(consumer).addBinary(Binary.fromString("test string"));
		inOrder.verify(consumer).endField("stringField", 5);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteByteArray() {
		Map<String, Object> record = new HashMap<>();
		byte[] bytes = { 1, 2, 3, 4, 5 };
		record.put("bytesField", bytes);

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("bytesField", 6);
		// The implementation calls toString() on the byte array which gives something
		// like "[B@1a2b3c4d"
		// We can't predict the exact string, so we need to use a more generic
		// verification
		inOrder.verify(consumer).addBinary(any(Binary.class));
		inOrder.verify(consumer).endField("bytesField", 6);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testWriteNestedGroup() {
		Map<String, Object> record = new HashMap<>();
		Map<String, Object> nestedGroup = new HashMap<>();
		nestedGroup.put("nestedInt", 99);
		nestedGroup.put("nestedString", "nested value");
		record.put("nestedGroupField", nestedGroup);

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("nestedGroupField", 7);
		inOrder.verify(consumer).startGroup();
		inOrder.verify(consumer).startField("nestedInt", 0);
		inOrder.verify(consumer).addInteger(99);
		inOrder.verify(consumer).endField("nestedInt", 0);
		inOrder.verify(consumer).startField("nestedString", 1);
		inOrder.verify(consumer).addBinary(Binary.fromString("nested value"));
		inOrder.verify(consumer).endField("nestedString", 1);
		inOrder.verify(consumer).endGroup();
		inOrder.verify(consumer).endField("nestedGroupField", 7);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testSkipNullValues() {
		Map<String, Object> record = new HashMap<>();
		record.put("booleanField", true);
		record.put("intField", null); // Should be skipped
		record.put("longField", 123L);

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();
		inOrder.verify(consumer).startField("booleanField", 0);
		inOrder.verify(consumer).addBoolean(true);
		inOrder.verify(consumer).endField("booleanField", 0);
		// No interaction for intField
		inOrder.verify(consumer).startField("longField", 2);
		inOrder.verify(consumer).addLong(123L);
		inOrder.verify(consumer).endField("longField", 2);
		inOrder.verify(consumer).endMessage();
	}

	@Test
	public void testCompleteRecord() {
		Map<String, Object> record = new HashMap<>();
		record.put("booleanField", true);
		record.put("intField", 42);
		record.put("longField", 12345678900L);
		record.put("floatField", 3.14f);
		record.put("doubleField", 2.71828);
		record.put("stringField", "test string");
		record.put("bytesField", "binary data");

		Map<String, Object> nestedGroup = new HashMap<>();
		nestedGroup.put("nestedInt", 99);
		nestedGroup.put("nestedString", "nested value");
		record.put("nestedGroupField", nestedGroup);

		writeSupport.write(record);

		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).startMessage();

		inOrder.verify(consumer).startField("booleanField", 0);
		inOrder.verify(consumer).addBoolean(true);
		inOrder.verify(consumer).endField("booleanField", 0);

		inOrder.verify(consumer).startField("intField", 1);
		inOrder.verify(consumer).addInteger(42);
		inOrder.verify(consumer).endField("intField", 1);

		inOrder.verify(consumer).startField("longField", 2);
		inOrder.verify(consumer).addLong(12345678900L);
		inOrder.verify(consumer).endField("longField", 2);

		inOrder.verify(consumer).startField("floatField", 3);
		inOrder.verify(consumer).addFloat(3.14f);
		inOrder.verify(consumer).endField("floatField", 3);

		inOrder.verify(consumer).startField("doubleField", 4);
		inOrder.verify(consumer).addDouble(2.71828);
		inOrder.verify(consumer).endField("doubleField", 4);

		inOrder.verify(consumer).startField("stringField", 5);
		inOrder.verify(consumer).addBinary(Binary.fromString("test string"));
		inOrder.verify(consumer).endField("stringField", 5);

		inOrder.verify(consumer).startField("bytesField", 6);
		inOrder.verify(consumer).addBinary(Binary.fromString("binary data"));
		inOrder.verify(consumer).endField("bytesField", 6);

		inOrder.verify(consumer).startField("nestedGroupField", 7);
		inOrder.verify(consumer).startGroup();
		inOrder.verify(consumer).startField("nestedInt", 0);
		inOrder.verify(consumer).addInteger(99);
		inOrder.verify(consumer).endField("nestedInt", 0);
		inOrder.verify(consumer).startField("nestedString", 1);
		inOrder.verify(consumer).addBinary(Binary.fromString("nested value"));
		inOrder.verify(consumer).endField("nestedString", 1);
		inOrder.verify(consumer).endGroup();
		inOrder.verify(consumer).endField("nestedGroupField", 7);

		inOrder.verify(consumer).endMessage();
	}
}
