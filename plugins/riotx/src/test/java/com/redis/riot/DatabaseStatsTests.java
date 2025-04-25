package com.redis.riot;

import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;

import com.redis.riot.DatabaseStats.Keyspace;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;

public class DatabaseStatsTests {

	@Test
	void testCount() throws Exception {
		long minMemory = 0;
		long maxMemory = 1000000;
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(10000);
		reader.open(new ExecutionContext());
		DatabaseStats stats = new DatabaseStats();
		Random random = new Random();
		KeyValue<String> item;
		while ((item = reader.read()) != null) {
			item.setMemoryUsage(random.nextLong(minMemory, maxMemory));
			stats.keyValue(item);
		}
		reader.close();
		long memorySpread = maxMemory - minMemory;
		List<Keyspace> keyspaces = stats.keyspaces();
		for (Keyspace keyspace : keyspaces) {
			Assertions.assertEquals(memorySpread / 2, keyspace.getMemoryUsage().quantile(.5), memorySpread / 10);
		}

	}

}
