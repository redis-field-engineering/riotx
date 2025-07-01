package com.redis.spring.batch.item.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.redis.batch.KeyStructEvent;
import com.redis.batch.KeyType;
import com.redis.batch.KeyTtlTypeEvent;
import com.redis.batch.gen.CollectionOptions;
import com.redis.batch.gen.Generator;
import com.redis.batch.gen.StreamOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;

class GeneratorTests {

    @Test
    void defaults() throws Exception {
        int count = 123;
        GeneratorItemReader reader = new GeneratorItemReader(new Generator());
        reader.setMaxItemCount(count);
        List<KeyStructEvent<String, String>> list = readAll(reader);
        Assertions.assertEquals(count, list.size());
    }

    private List<KeyStructEvent<String, String>> readAll(GeneratorItemReader reader) throws Exception {
        List<KeyStructEvent<String, String>> list = new ArrayList<>();
        KeyStructEvent<String, String> ds;
        while ((ds = reader.read()) != null) {
            list.add(ds);
        }
        return list;
    }

    @Test
    void types() throws Exception {
        int size = Generator.defaultTypes().size();
        int count = size * 100;
        GeneratorItemReader reader = new GeneratorItemReader(new Generator());
        reader.setMaxItemCount(count);
        List<KeyStructEvent<String, String>> items = readAll(reader);
        Map<KeyType, List<KeyStructEvent<String, String>>> byType = items.stream()
                .collect(Collectors.groupingBy(KeyStructEvent::getType));
        for (List<KeyStructEvent<String, String>> values : byType.values()) {
            Assertions.assertEquals(count / size, values.size());
        }
    }

    @Test
    void options() throws Exception {
        int count = 123;
        GeneratorItemReader reader = new GeneratorItemReader(new Generator());
        reader.setMaxItemCount(count);
        List<KeyStructEvent<String, String>> list = readAll(reader);
        Assertions.assertEquals(count, list.size());
        for (KeyStructEvent<String, String> ds : list) {
            switch (ds.getType()) {
                case set:
                    Assertions.assertEquals(CollectionOptions.DEFAULT_MEMBER_COUNT.getMax(), ds.asSet().size());
                    break;
                case list:
                    Assertions.assertEquals(CollectionOptions.DEFAULT_MEMBER_COUNT.getMax(), ds.asList().size());
                    break;
                case zset:
                    Assertions.assertEquals(CollectionOptions.DEFAULT_MEMBER_COUNT.getMax(), ds.asZSet().size());
                    break;
                case stream:
                    Assertions.assertEquals(StreamOptions.DEFAULT_MESSAGE_COUNT.getMax(), ds.asStream().size());
                    break;
                default:
                    break;
            }
        }
    }

    @Test
    void keys() throws Exception {
        GeneratorItemReader reader = new GeneratorItemReader(new Generator());
        reader.setMaxItemCount(10);
        reader.open(new ExecutionContext());
        KeyStructEvent<String, String> keyValueEvent = reader.read();
        Assertions.assertEquals(
                Generator.DEFAULT_KEYSPACE + Generator.DEFAULT_KEY_SEPARATOR + Generator.DEFAULT_KEY_RANGE.getMin(),
                keyValueEvent.getKey());
        String lastKey;
        do {
            lastKey = keyValueEvent.getKey();
        } while ((keyValueEvent = reader.read()) != null);
        Assertions.assertEquals(Generator.DEFAULT_KEYSPACE + Generator.DEFAULT_KEY_SEPARATOR + 10, lastKey);
    }

    @Test
    void read() throws Exception {
        int count = 456;
        GeneratorItemReader reader = new GeneratorItemReader(new Generator());
        reader.open(new ExecutionContext());
        reader.setMaxItemCount(456);
        KeyTtlTypeEvent<String> ds1 = reader.read();
        assertEquals("gen:1", ds1.getKey());
        int actualCount = 1;
        while (reader.read() != null) {
            actualCount++;
        }
        assertEquals(count, actualCount);
        reader.close();
    }

}
