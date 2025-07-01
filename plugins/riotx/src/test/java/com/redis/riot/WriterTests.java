package com.redis.riot;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.batch.BatchUtils;
import com.redis.batch.KeyStructEvent;
import com.redis.batch.KeyType;
import com.redis.riot.file.xml.XmlResourceItemWriter;
import com.redis.riot.file.xml.XmlResourceItemWriterBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.core.io.FileSystemResource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class WriterTests {

    @Test
    void test() throws Exception {
        Path directory = Files.createTempDirectory(getClass().getName());
        Path file = directory.resolve("redis.xml");
        XmlMapper mapper = new XmlMapper();
        BatchUtils.configureObjectMapper(mapper);
        mapper.setConfig(mapper.getSerializationConfig().withRootName("record"));
        JacksonJsonObjectMarshaller<KeyStructEvent<String, String>> marshaller = new JacksonJsonObjectMarshaller<>();
        marshaller.setObjectMapper(mapper);
        XmlResourceItemWriter<KeyStructEvent<String, String>> writer = new XmlResourceItemWriterBuilder<KeyStructEvent<String, String>>().name(
                        "xml-resource-writer").resource(new FileSystemResource(file)).rootName("root").xmlObjectMarshaller(marshaller)
                .build();
        writer.afterPropertiesSet();
        writer.open(new ExecutionContext());
        KeyStructEvent<String, String> item1 = new KeyStructEvent<>();
        item1.setKey("key1");
        Instant instant1 = Instant.now();
        item1.setTtl(instant1);
        item1.setType(KeyType.hash);
        Map<String, String> hash1 = Map.of("field1", "value1", "field2", "value2");
        item1.setValue(hash1);
        KeyStructEvent<String, String> item2 = new KeyStructEvent<>();
        item2.setKey("key2");
        Instant instant2 = Instant.now();
        item2.setTtl(instant2);
        item2.setType(KeyType.hash);
        Map<String, String> hash2 = Map.of("field1", "value1", "field2", "value2");
        item2.setValue(hash2);
        writer.write(Chunk.of(item1, item2));
        writer.close();
        ObjectReader reader = mapper.readerFor(KeyStructEvent.class);
        List<KeyStructEvent<String, String>> keyValueEvents = reader.<KeyStructEvent<String, String>> readValues(file.toFile())
                .readAll();
        Assertions.assertEquals(2, keyValueEvents.size());
        Assertions.assertEquals(item1.getKey(), keyValueEvents.get(0).getKey());
        Assertions.assertEquals(item2.getKey(), keyValueEvents.get(1).getKey());
        Assertions.assertEquals(item1.getTtl(), keyValueEvents.get(0).getTtl());
        Assertions.assertEquals(item2.getTtl(), keyValueEvents.get(1).getTtl());
        Assertions.assertEquals(item1.getValue(), keyValueEvents.get(0).getValue());
        Assertions.assertEquals(item2.getValue(), keyValueEvents.get(1).getValue());
        Assertions.assertEquals(instant1, keyValueEvents.get(0).getTtl());
    }

}
