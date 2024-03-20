package com.redis.spring.batch.memcached.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;

import com.redis.spring.batch.memcached.MemcachedEntry;
import com.redis.spring.batch.memcached.MemcachedItemReader;
import com.redis.spring.batch.memcached.MemcachedItemWriter;
import com.redis.spring.batch.memcached.impl.ByteArrayTranscoder;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

@TestInstance(Lifecycle.PER_CLASS)
public class MemcachedBatchTests {

	private static final Transcoder<byte[]> transcoder = new ByteArrayTranscoder();

	private MemcachedClient client;
	private List<InetSocketAddress> addresses;
	private MemcachedContainer container;

	public static <T> List<T> readAll(ItemReader<T> reader) throws Exception {
		List<T> list = new ArrayList<>();
		T element;
		while ((element = reader.read()) != null) {
			list.add(element);
		}
		return list;
	}

	@BeforeAll
	void setup() throws IOException {
		container = new MemcachedContainer(
				MemcachedContainer.DEFAULT_IMAGE_NAME.withTag(MemcachedContainer.DEFAULT_TAG));
		container.start();
		addresses = AddrUtil.getAddresses(container.getAddress());
		client = new MemcachedClient(addresses);
	}

	@AfterAll
	void teardown() {
		if (client != null) {
			client.shutdown();
		}
		if (container != null) {
			container.stop();
		}
	}

	@Test
	void reader() throws Exception {
		long startTime = System.currentTimeMillis() / 1000;
		int count = 1867;
		Map<String, MemcachedEntry> entries = entries(count)
				.collect(Collectors.toMap(MemcachedEntry::getKey, Function.identity()));
		entries.values().forEach(e -> client.set(e.getKey(), e.getExpiration(), e.getValue(), transcoder));
		MemcachedItemReader reader = new MemcachedItemReader(addresses);
		reader.open(new ExecutionContext());
		List<MemcachedEntry> actualEntries = readAll(reader);
		Assertions.assertEquals(count, actualEntries.size());
		actualEntries.forEach(e -> {
			MemcachedEntry entry = entries.get(e.getKey());
			Assertions.assertArrayEquals(entry.getValue(), e.getValue());
			Assertions.assertTrue(Math.abs(startTime + entry.getExpiration() - e.getExpiration()) < 30);
		});
		reader.close();
	}

	private Stream<MemcachedEntry> entries(int count) {
		return IntStream.range(0, count).mapToObj(this::entry);
	}

	private MemcachedEntry entry(int index) {
		MemcachedEntry entry = new MemcachedEntry();
		entry.setKey("key:" + index);
		entry.setExpiration(1200 + index);
		entry.setValue(("value:" + index).getBytes());
		return entry;
	}

	@Test
	void writer() throws Exception {
		MemcachedItemWriter writer = new MemcachedItemWriter(addresses);
		writer.open(new ExecutionContext());
		int count = 1867;
		List<MemcachedEntry> entries = entries(count).collect(Collectors.toList());
		writer.write(new Chunk<>(entries));
		writer.close();
		MemcachedItemReader reader = new MemcachedItemReader(addresses);
		reader.open(new ExecutionContext());
		List<MemcachedEntry> actualEntries = readAll(reader);
		Assertions.assertEquals(count, actualEntries.size());
		reader.close();
	}

}
