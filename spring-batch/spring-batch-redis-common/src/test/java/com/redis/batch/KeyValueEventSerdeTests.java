package com.redis.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.redis.batch.gen.Generator;
import com.redis.lettucemod.timeseries.Sample;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.time.Instant;
import java.util.Arrays;

@TestInstance(Lifecycle.PER_CLASS)
class KeyValueEventSerdeTests {

    private static final String timeseries = "{\"key\":\"gen:97\",\"timestamp\":\"2024-11-06T08:23:12.559Z\", \"type\":\"timeseries\",\"value\":[{\"timestamp\":1695939533285,\"value\":0.07027403662738285},{\"timestamp\":1695939533286,\"value\":0.7434808603018632},{\"timestamp\":1695939533287,\"value\":0.36481049906367213},{\"timestamp\":1695939533288,\"value\":0.08986928499552382},{\"timestamp\":1695939533289,\"value\":0.3901401870373925},{\"timestamp\":1695939533290,\"value\":0.1088584873055678},{\"timestamp\":1695939533291,\"value\":0.5649631025302376},{\"timestamp\":1695939533292,\"value\":0.9284983053028953},{\"timestamp\":1695939533293,\"value\":0.5009349293022067},{\"timestamp\":1695939533294,\"value\":0.7798297389022721}],\"memoryUsage\":0}";

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    void setup() {
        mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
        BatchUtils.configureObjectMapper(mapper);
    }

    @SuppressWarnings("unchecked")
    @Test
    void deserialize() throws JsonProcessingException {
        KeyStructEvent<String, String> keyValueEvent = mapper.readValue(timeseries, KeyStructEvent.class);
        Assertions.assertEquals("gen:97", keyValueEvent.getKey());
    }

    @Test
    void serialize() throws JsonProcessingException {
        String key = "ts:1";
        Instant ttl = Instant.now();
        KeyStructEvent<String, String> ts = new KeyStructEvent<>();
        ts.setKey(key);
        ts.setTtl(ttl);
        ts.setType(KeyType.timeseries);
        Sample sample1 = Sample.of(Instant.now().toEpochMilli(), 123.456);
        Sample sample2 = Sample.of(Instant.now().toEpochMilli() + 1000, 456.123);
        ts.setValue(Arrays.asList(sample1, sample2));
        String json = mapper.writeValueAsString(ts);
        JsonNode jsonNode = mapper.readTree(json);
        Assertions.assertEquals(key, jsonNode.get("key").asText());
        ArrayNode valueNode = (ArrayNode) jsonNode.get("value");
        Assertions.assertEquals(2, valueNode.size());
        Assertions.assertEquals(sample2.getValue(), (valueNode.get(1).get("value")).asDouble());
    }

    @SuppressWarnings("unchecked")
    @Test
    void serde(TestInfo info) throws Exception {
        Generator reader = new Generator();
        for (int i = 0; i < 17; i++) {
            KeyStructEvent<String, String> item = reader.next();
            String json = mapper.writeValueAsString(item);
            KeyStructEvent<String, String> result = mapper.readValue(json, KeyStructEvent.class);
            assertEquals(item, result);
        }
    }

    private <K, V> void assertEquals(KeyStructEvent<K, V> source, KeyStructEvent<K, V> target) {
        Assertions.assertEquals(source.getTtl(), target.getTtl());
        Assertions.assertEquals(source.getType(), target.getType());
        Assertions.assertEquals(source.getKey(), target.getKey());
        Assertions.assertEquals(source.getValue(), target.getValue());
    }

}
