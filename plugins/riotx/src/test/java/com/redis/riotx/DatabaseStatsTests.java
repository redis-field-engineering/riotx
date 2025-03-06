package com.redis.riotx;

import java.util.Random;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;

import com.redis.riotx.stats.DatabaseAnalyzer;
import com.redis.riotx.stats.KeyTypeStats;
import com.redis.riotx.stats.KeyspaceStats;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;

public class DatabaseStatsTests {

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Test
	void testCount() throws Exception {
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(1000);
		reader.open(new ExecutionContext());
		DatabaseAnalyzer analyzer = new DatabaseAnalyzer();
		Random random = new Random();
		KeyValue<String> item;
		while ((item = reader.read()) != null) {
			item.setTtl(random.nextLong(1000));
			item.setMemoryUsage(random.nextLong(1000000));
			analyzer.add(item);
		}
		for (KeyspaceStats keyspace : analyzer.keyspaceStats()) {
			for (KeyTypeStats typeStats : keyspace.typeStats()) {
				log.info("Stats: {}", typeStats);
			}
		}
		reader.close();
	}

}
