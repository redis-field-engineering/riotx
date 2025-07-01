package com.redis.riot;

import com.redis.batch.KeyStatEvent;
import com.redis.batch.KeyTtlTypeEvent;
import com.redis.batch.gen.Generator;
import com.redis.spring.batch.item.redis.GeneratorItemReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;

import java.util.List;
import java.util.Random;

public class DatabaseStatsTests {

    @Test
    void testCount() throws Exception {
        long minMemory = 0;
        long maxMemory = 1000000;
        GeneratorItemReader reader = new GeneratorItemReader(new Generator());
        reader.setMaxItemCount(10000);
        reader.open(new ExecutionContext());
        RedisStats stats = new RedisStats();
        Random random = new Random();
        KeyTtlTypeEvent<String> item;
        while ((item = reader.read()) != null) {
            KeyStatEvent<String> statEvent = new KeyStatEvent<>();
            statEvent.setKey(item.getKey());
            statEvent.setTtl(item.getTtl());
            statEvent.setEvent(item.getEvent());
            statEvent.setType(item.getType());
            statEvent.setOperation(item.getOperation());
            statEvent.setTimestamp(item.getTimestamp());
            statEvent.setMemoryUsage(random.nextLong(minMemory, maxMemory));
            stats.onStatEvent(statEvent);
        }
        reader.close();
        long memorySpread = maxMemory - minMemory;
        List<RedisStats.Keyspace> keyspaces = stats.keyspaces();
        for (RedisStats.Keyspace keyspace : keyspaces) {
            Assertions.assertEquals(memorySpread / 2, keyspace.getMemoryUsage().quantile(.5), memorySpread / 10);
        }

    }

}
