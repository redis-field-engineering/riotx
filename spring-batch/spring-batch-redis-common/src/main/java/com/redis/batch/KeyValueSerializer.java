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
public class KeyValueSerializer extends StdSerializer<KeyValue> {

    public KeyValueSerializer() {
        this(null);
    }

    public KeyValueSerializer(Class<KeyValue> t) {
        super(t);
    }

    @Override
    public void serialize(KeyValue kv, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();

        gen.writeStringField(KeyValueDeserializer.KEY, (String) kv.getKey());

        if (kv.getType() != null) {
            gen.writeStringField(KeyValueDeserializer.TYPE, kv.getType());
        }

        if (kv.getTtl() != null) {
            gen.writeStringField(KeyValueDeserializer.TTL, kv.getTtl().toString());
        }

        if (kv.getMemoryUsage() != 0) {
            gen.writeNumberField(KeyValueDeserializer.MEMORY_USAGE, kv.getMemoryUsage());
        }

        Object value = kv.getValue();
        if (value != null) {
            gen.writeFieldName(KeyValueDeserializer.VALUE);
            serializeValue(gen, serializers, kv.getType(), value);
        }

        gen.writeEndObject();
    }

    @SuppressWarnings("unchecked")
    private void serializeValue(JsonGenerator gen, SerializerProvider serializers, String type, Object value)
            throws IOException {
        switch (type) {
            case KeyValue.TYPE_STREAM:
                gen.writeStartArray();
                for (StreamMessage<String, String> message : (Collection<StreamMessage<String, String>>) value) {
                    gen.writeStartObject();
                    gen.writeStringField(KeyValueDeserializer.STREAM, message.getStream());
                    gen.writeStringField(KeyValueDeserializer.ID, message.getId());
                    gen.writeObjectField(KeyValueDeserializer.BODY, message.getBody());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                break;

            case KeyValue.TYPE_ZSET:
                gen.writeStartArray();
                for (ScoredValue<String> sv : (Set<ScoredValue<String>>) value) {
                    gen.writeStartObject();
                    gen.writeNumberField(KeyValueDeserializer.SCORE, sv.getScore());
                    gen.writeStringField(KeyValueDeserializer.VALUE, sv.getValue());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                break;

            case KeyValue.TYPE_TIMESERIES:
                gen.writeStartArray();
                for (Sample sample : (Collection<Sample>) value) {
                    gen.writeStartObject();
                    gen.writeNumberField(KeyValueDeserializer.TIMESTAMP, sample.getTimestamp());
                    gen.writeNumberField(KeyValueDeserializer.VALUE, sample.getValue());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                break;

            case KeyValue.TYPE_HASH:
                serializers.defaultSerializeValue((Map<?, ?>) value, gen);
                break;

            case KeyValue.TYPE_STRING:
            case KeyValue.TYPE_JSON:
                gen.writeString((String) value);
                break;

            case KeyValue.TYPE_LIST:
            case KeyValue.TYPE_SET:
                serializers.defaultSerializeValue(value, gen);
                break;

            default:
                serializers.defaultSerializeNull(gen);
        }
    }

}
