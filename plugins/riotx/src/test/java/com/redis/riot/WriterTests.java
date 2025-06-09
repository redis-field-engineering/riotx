package com.redis.riot;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.redis.batch.KeyValue;
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
        mapper.registerModule(new JavaTimeModule());
        mapper.setConfig(mapper.getSerializationConfig().withRootName("record"));
        JacksonJsonObjectMarshaller<KeyValue<String>> marshaller = new JacksonJsonObjectMarshaller<>();
        marshaller.setObjectMapper(mapper);
        XmlResourceItemWriter<KeyValue<String>> writer = new XmlResourceItemWriterBuilder<KeyValue<String>>().name(
                        "xml-resource-writer").resource(new FileSystemResource(file)).rootName("root").xmlObjectMarshaller(marshaller)
                .build();
        writer.afterPropertiesSet();
        writer.open(new ExecutionContext());
        KeyValue<String> item1 = new KeyValue<>();
        item1.setKey("key1");
        Instant instant1 = Instant.now();
        item1.setTtl(instant1);
        item1.setType(KeyValue.TYPE_HASH);
        Map<String, String> hash1 = Map.of("field1", "value1", "field2", "value2");
        item1.setValue(hash1);
        KeyValue<String> item2 = new KeyValue<>();
        item2.setKey("key2");
        Instant instant2 = Instant.now();
        item2.setTtl(instant2);
        item2.setType(KeyValue.TYPE_STREAM);
        Map<String, String> hash2 = Map.of("field1", "value1", "field2", "value2");
        item2.setValue(hash2);
        writer.write(Chunk.of(item1, item2));
        writer.close();
        ObjectReader reader = mapper.readerFor(KeyValue.class);
        List<KeyValue<String>> keyValues = reader.<KeyValue<String>> readValues(file.toFile()).readAll();
        Assertions.assertEquals(2, keyValues.size());
        Assertions.assertEquals(item1.getKey(), keyValues.get(0).getKey());
        Assertions.assertEquals(item2.getKey(), keyValues.get(1).getKey());
        Assertions.assertEquals(item1.getTtl(), keyValues.get(0).getTtl());
        Assertions.assertEquals(item2.getTtl(), keyValues.get(1).getTtl());
        Assertions.assertEquals(item1.getValue(), keyValues.get(0).getValue());
        Assertions.assertEquals(item2.getValue(), keyValues.get(1).getValue());
        Assertions.assertEquals(instant1, keyValues.get(0).getTtl());
    }

}
