package com.redis.batch;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("rawtypes")
public class KeyValueSerializer extends StdSerializer<KeyValueEvent> {

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

    public KeyValueSerializer() {
        this(null);
    }

    public KeyValueSerializer(Class<KeyValueEvent> t) {
        super(t);
    }

    @Override
    public void serialize(KeyValueEvent kv, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();

        gen.writeStringField(KEY, (String) kv.getKey());

        if (kv.getTimestamp() != null) {
            gen.writeStringField(TIMESTAMP, kv.getTimestamp().toString());
        }

        if (kv.getEvent() != null) {
            gen.writeStringField(EVENT, kv.getEvent());
        }

        if (kv.getType() != null) {
            gen.writeStringField(TYPE, kv.getType());
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
            serializeValue(gen, serializers, KeyType.of(kv.getType()), value);
        }

        gen.writeEndObject();
    }

    @SuppressWarnings("unchecked")
    private void serializeValue(JsonGenerator gen, SerializerProvider serializers, KeyType type, Object value)
            throws IOException {
        switch (type) {
            case STREAM:
                gen.writeStartArray();
                for (StreamMessage<String, String> message : (Collection<StreamMessage<String, String>>) value) {
                    gen.writeStartObject();
                    gen.writeStringField(STREAM, message.getStream());
                    gen.writeStringField(ID, message.getId());
                    gen.writeObjectField(BODY, message.getBody());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                break;

            case ZSET:
                gen.writeStartArray();
                for (ScoredValue<String> sv : (Set<ScoredValue<String>>) value) {
                    gen.writeStartObject();
                    gen.writeNumberField(SCORE, sv.getScore());
                    gen.writeStringField(VALUE, sv.getValue());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                break;

            case TIMESERIES:
                gen.writeStartArray();
                for (Sample sample : (Collection<Sample>) value) {
                    gen.writeStartObject();
                    gen.writeNumberField(TIMESTAMP, sample.getTimestamp());
                    gen.writeNumberField(VALUE, sample.getValue());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                break;

            case HASH:
                serializers.defaultSerializeValue((Map<?, ?>) value, gen);
                break;

            case STRING:
            case JSON:
                gen.writeString((String) value);
                break;

            case LIST:
            case SET:
                serializers.defaultSerializeValue(value, gen);
                break;

            default:
                serializers.defaultSerializeNull(gen);
        }
    }

}
