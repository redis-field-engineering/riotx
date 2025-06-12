package com.redis.riot.core.function;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.batch.KeyValue;
import io.lettuce.core.StreamMessage;
import org.springframework.batch.item.ItemProcessor;

import java.util.LinkedHashMap;
import java.util.Map;

public class KeyValueToStreamMessage implements ItemProcessor<KeyValue<String>, StreamMessage<String, String>> {

    private final ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);

    private final String stream;

    public KeyValueToStreamMessage(String stream) {
        this.stream = stream;
    }

    @Override
    public StreamMessage<String, String> process(KeyValue<String> struct) throws Exception {
        Map<String, String> body = new LinkedHashMap<>();
        body.put("key", struct.getKey());
        body.put("time", String.valueOf(struct.getTime()));
        body.put("type", struct.getType());
        body.put("ttl", String.valueOf(struct.getTtl()));
        body.put("mem", String.valueOf(struct.getMemoryUsage()));
        body.put("value", value(struct));
        return new StreamMessage<>(stream, null, body);
    }

    private String value(KeyValue<String> struct) throws JsonProcessingException {
        if (struct.getType() == null) {
            return null;
        }
        switch (struct.type()) {
            case STRING:
            case JSON:
                return (String) struct.getValue();
            default:
                return mapper.writeValueAsString(struct.getValue());
        }
    }

}
