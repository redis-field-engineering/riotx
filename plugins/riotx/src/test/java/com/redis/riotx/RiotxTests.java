package com.redis.riotx;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.Beers;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.ItemType;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.sync.RedisCommands;

abstract class RiotxTests extends AbstractRiotxApplicationTestBase {

	private static final Integer BEER_COUNT = 1019;

	@BeforeAll
	void setDefaults() {
		setIdleTimeout(Duration.ofSeconds(1));
	}

	@Test
	void redisImportJson(TestInfo info) throws Exception {
		int count = 1000;
		GeneratorItemReader generator = generator(count, ItemType.HASH);
		generator.setKeyspace("hash");
		generate(info, generator);
		String filename = "redis-import-json";
		execute(info, filename);
		List<String> keys = targetRedisCommands.keys("doc:*");
		Assertions.assertEquals(count, keys.size());
		Assertions.assertEquals(KeyValue.TYPE_JSON, targetRedisCommands.type(keys.get(0)));
		String json = targetRedisCommands.jsonGet(keys.get(1)).get(0).toString();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node = mapper.readTree(json);
		Assertions.assertNull(node.get("id"));
		Assertions.assertNotNull(node.get("field1"));
	}

	@Test
	void streamImport(TestInfo info) throws Exception {
		String stream = "stream:beers";
		populateStream(stream);
		String filename = "stream-import-hset";
		execute(info, filename);
		assertStreamImport(redisCommands);
	}

	@Test
	void streamImportTarget(TestInfo info) throws Exception {
		String stream = "stream:beers";
		populateStream(stream);
		String filename = "stream-import-target-hset";
		execute(info, filename);
		assertStreamImport(targetRedisCommands);
	}

	@Test
	void parquetFileImport(TestInfo info) throws Exception {
		String filename = "file-import-parquet";
		execute(info, filename);
		Assertions.assertEquals(1000, redisCommands.dbsize());
	}

	private static void assertStreamImport(RedisCommands<String, String> commands) throws IOException {
		List<String> keys = commands.keys("beer:*");
		Assertions.assertEquals(BEER_COUNT, keys.size());
		Map<String, Object> expected = Beers.mapIterator().next();
		Map<String, String> actual = commands.hgetall("beer:" + expected.get("id"));
		Assertions.assertEquals(expected, actual);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private int populateStream(String stream) throws IOException {
		List<RedisFuture<?>> futures = new ArrayList<>();
		int count = 0;
		try {
			MappingIterator<Map<String, Object>> iterator = Beers.mapIterator();
			while (iterator.hasNext()) {
				Map<String, String> beer = (Map) iterator.next();
				futures.add(redisAsyncCommands.xadd(stream, beer));
				count++;
			}
			redisConnection.flushCommands();
			LettuceFutures.awaitAll(redisConnection.getTimeout(), futures.toArray(new RedisFuture[0]));
		} finally {
			redisConnection.setAutoFlushCommands(true);
		}
		return count;
	}

}
