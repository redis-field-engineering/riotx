package com.redis.batch;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

import java.io.IOException;

@SuppressWarnings("rawtypes")
public class KeyStructSerializer extends StdSerializer<KeyStructEvent> {

    public static final String KEY = "key";

    public static final String EVENT = "event";

    public static final String TYPE = "type";

    public static final String OPERATION = "operation";

    public static final String VALUE = "value";

    public static final String SCORE = "score";

    public static final String TTL = "ttl";

    public static final String STREAM = "stream";

    public static final String ID = "id";

    public static final String BODY = "body";

    public static final String TIMESTAMP = "timestamp";

    public KeyStructSerializer() {
        this(null);
    }

    public KeyStructSerializer(Class<KeyStructEvent> t) {
        super(t);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void serialize(KeyStructEvent kv, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();

        gen.writeStringField(KEY, (String) kv.getKey());

        if (kv.getTimestamp() != null) {
            gen.writeStringField(TIMESTAMP, kv.getTimestamp().toString());
        }

        if (kv.getEvent() != null) {
            gen.writeStringField(EVENT, kv.getEvent());
        }

        if (kv.getType() != null) {
            gen.writeStringField(TYPE, kv.getType().name());
        }

        if (kv.getTtl() != null) {
            gen.writeStringField(TTL, kv.getTtl().toString());
        }

        if (kv.getOperation() != null) {
            gen.writeStringField(OPERATION, kv.getOperation().name());
        }

        Object value = kv.getValue();
        if (value != null) {
            gen.writeFieldName(VALUE);
            serializeValue(gen, serializers, (KeyStructEvent<String, String>) kv);
        }

        gen.writeEndObject();
    }

    private void serializeValue(JsonGenerator gen, SerializerProvider serializers, KeyStructEvent<String, String> event)
            throws IOException {
        switch (event.getType()) {
            case stream:
                gen.writeStartArray();
                for (StreamMessage<String, String> message : event.asStream()) {
                    gen.writeStartObject();
                    gen.writeStringField(STREAM, message.getStream());
                    gen.writeStringField(ID, message.getId());
                    gen.writeObjectField(BODY, message.getBody());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                break;

            case zset:
                gen.writeStartArray();
                for (ScoredValue<String> sv : event.asZSet()) {
                    gen.writeStartObject();
                    gen.writeNumberField(SCORE, sv.getScore());
                    gen.writeStringField(VALUE, sv.getValue());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                break;

            case timeseries:
                gen.writeStartArray();
                for (Sample sample : event.asTimeseries()) {
                    gen.writeStartObject();
                    gen.writeNumberField(TIMESTAMP, sample.getTimestamp());
                    gen.writeNumberField(VALUE, sample.getValue());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                break;

            case hash:
                serializers.defaultSerializeValue(event.asHash(), gen);
                break;

            case string:
                gen.writeString(event.asString());
                break;
            case json:
                gen.writeString(event.asJson());
                break;

            case list:
                serializers.defaultSerializeValue(event.asList(), gen);
                break;
            case set:
                serializers.defaultSerializeValue(event.asSet(), gen);
                break;

            default:
                serializers.defaultSerializeNull(gen);
        }
    }

}
