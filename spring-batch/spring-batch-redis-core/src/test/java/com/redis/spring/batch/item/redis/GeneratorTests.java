package com.redis.spring.batch.item.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.gen.CollectionOptions;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.StreamOptions;

class GeneratorTests {

	@Test
	void defaults() throws UnexpectedInputException, ParseException, Exception {
		int count = 123;
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(count);
		List<KeyValue<String>> list = readAll(reader);
		Assertions.assertEquals(count, list.size());
	}

	private List<KeyValue<String>> readAll(GeneratorItemReader reader)
			throws UnexpectedInputException, ParseException, Exception {
		List<KeyValue<String>> list = new ArrayList<>();
		KeyValue<String> ds;
		while ((ds = reader.read()) != null) {
			list.add(ds);
		}
		return list;
	}

	@Test
	void types() throws UnexpectedInputException, ParseException, Exception {
		int size = GeneratorItemReader.defaultTypes().size();
		int count = size * 100;
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(count);
		List<KeyValue<String>> items = readAll(reader);
		Map<String, List<KeyValue<String>>> byType = items.stream().collect(Collectors.groupingBy(KeyValue::getType));
		for (List<KeyValue<String>> values : byType.values()) {
			Assertions.assertEquals(count / size, values.size());
		}
	}

	@Test
	void options() throws Exception {
		int count = 123;
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(count);
		List<KeyValue<String>> list = readAll(reader);
		Assertions.assertEquals(count, list.size());
		for (KeyValue<String> ds : list) {
			switch (ds.getType()) {
			case KeyValue.TYPE_SET:
			case KeyValue.TYPE_LIST:
			case KeyValue.TYPE_ZSET:
				Assertions.assertEquals(CollectionOptions.DEFAULT_MEMBER_COUNT.getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			case KeyValue.TYPE_STREAM:
				Assertions.assertEquals(StreamOptions.DEFAULT_MESSAGE_COUNT.getMax(),
						((Collection<?>) ds.getValue()).size());
				break;
			default:
				break;
			}
		}
	}

	@Test
	void keys() throws Exception {
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(10);
		reader.open(new ExecutionContext());
		KeyValue<String> keyValue = reader.read();
		Assertions.assertEquals(GeneratorItemReader.DEFAULT_KEYSPACE + GeneratorItemReader.DEFAULT_KEY_SEPARATOR
				+ GeneratorItemReader.DEFAULT_KEY_RANGE.getMin(), keyValue.getKey());
		String lastKey;
		do {
			lastKey = keyValue.getKey();
		} while ((keyValue = reader.read()) != null);
		Assertions.assertEquals(GeneratorItemReader.DEFAULT_KEYSPACE + GeneratorItemReader.DEFAULT_KEY_SEPARATOR + 10,
				lastKey);
	}

	@Test
	void read() throws Exception {
		int count = 456;
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.open(new ExecutionContext());
		reader.setMaxItemCount(456);
		KeyValue<String> ds1 = reader.read();
		assertEquals("gen:1", ds1.getKey());
		int actualCount = 1;
		while (reader.read() != null) {
			actualCount++;
		}
		assertEquals(count, actualCount);
		reader.close();
	}

}
