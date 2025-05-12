package com.redis.riot.rdi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.lettuce.core.StreamMessage;
import org.springframework.batch.item.ItemProcessor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ChangeEventToStreamMessage implements ItemProcessor<ChangeEvent, StreamMessage<String, String>> {

    public static final String KEY = "key";

    public static final String VALUE = "value";

    private final ObjectMapper mapper = new ObjectMapper();

    private final ObjectWriter writer = mapper.writerFor(ChangeEventValue.class).withDefaultPrettyPrinter();

    private final Function<ChangeEvent, String> streamFunction;

    private Function<ChangeEvent, String> idFunction = m -> null;

    public ChangeEventToStreamMessage(String stream) {
        this(m -> stream);
    }

    public ChangeEventToStreamMessage(Function<ChangeEvent, String> streamFunction) {
        this.streamFunction = streamFunction;
    }

    public void setIdFunction(Function<ChangeEvent, String> function) {
        this.idFunction = function;
    }

    @Override
    public StreamMessage<String, String> process(ChangeEvent item) throws JsonProcessingException {
        String bodyKey = mapper.writeValueAsString(item.getKey());
        String bodyValue = writer.writeValueAsString(item.getValue());
        Map<String, String> body = new HashMap<>();
        body.put(KEY, bodyKey);
        body.put(VALUE, bodyValue);
        return new StreamMessage<>(streamFunction.apply(item), idFunction.apply(item), body);
    }

}

