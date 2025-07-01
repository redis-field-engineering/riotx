package com.redis.riot.core.function;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.batch.KeyStructEvent;
import io.lettuce.core.StreamMessage;
import org.springframework.batch.item.ItemProcessor;

import java.util.LinkedHashMap;
import java.util.Map;

public class KeyStructEventToStreamMessage
        implements ItemProcessor<KeyStructEvent<String, String>, StreamMessage<String, String>> {

    private final ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);

    private final String stream;

    public KeyStructEventToStreamMessage(String stream) {
        this.stream = stream;
    }

    @Override
    public StreamMessage<String, String> process(KeyStructEvent<String, String> struct) throws Exception {
        Map<String, String> body = new LinkedHashMap<>();
        body.put("key", struct.getKey());
        body.put("time", String.valueOf(struct.getTimestamp()));
        body.put("type", struct.getType().name());
        body.put("ttl", String.valueOf(struct.getTtl()));
        body.put("value", value(struct));
        return new StreamMessage<>(stream, null, body);
    }

    private String value(KeyStructEvent<String, String> struct) throws JsonProcessingException {
        if (struct.isString()) {
            return struct.asString();
        }
        if (struct.isJson()) {
            return struct.asJson();
        }
        return mapper.writeValueAsString(struct.getValue());
    }

}
