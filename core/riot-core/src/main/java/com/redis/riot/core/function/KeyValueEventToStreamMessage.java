package com.redis.riot.core.function;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.batch.KeyType;
import com.redis.batch.KeyValueEvent;
import io.lettuce.core.StreamMessage;
import org.springframework.batch.item.ItemProcessor;

import java.util.LinkedHashMap;
import java.util.Map;

public class KeyValueEventToStreamMessage implements ItemProcessor<KeyValueEvent<String>, StreamMessage<String, String>> {

    private final ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);

    private final String stream;

    public KeyValueEventToStreamMessage(String stream) {
        this.stream = stream;
    }

    @Override
    public StreamMessage<String, String> process(KeyValueEvent<String> struct) throws Exception {
        Map<String, String> body = new LinkedHashMap<>();
        body.put("key", struct.getKey());
        body.put("time", String.valueOf(struct.getTimestamp()));
        body.put("type", struct.getType());
        body.put("ttl", String.valueOf(struct.getTtl()));
        body.put("value", value(struct));
        return new StreamMessage<>(stream, null, body);
    }

    private String value(KeyValueEvent<String> struct) throws JsonProcessingException {
        KeyType keyType = KeyType.of(struct.getType());
        if (keyType == null) {
            return null;
        }
        switch (keyType) {
            case STRING:
            case JSON:
                return (String) struct.getValue();
            default:
                return mapper.writeValueAsString(struct.getValue());
        }
    }

}
