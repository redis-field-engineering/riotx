package com.redis.batch;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import static com.redis.batch.KeyStructSerializer.*;

@SuppressWarnings("rawtypes")
public class KeyStructDeserializer extends StdDeserializer<KeyStructEvent> {

    public KeyStructDeserializer() {
        this(null);
    }

    public KeyStructDeserializer(Class<KeyTtlTypeEvent<String>> t) {
        super(t);
    }

    @Override
    public KeyStructEvent<String, String> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = p.getCodec().readTree(p);
        KeyStructEvent<String, String> keyValueEvent = new KeyStructEvent<>();
        if (node.has(KEY)) {
            keyValueEvent.setKey(node.get(KEY).asText());
        }
        if (node.has(TIMESTAMP)) {
            keyValueEvent.setTimestamp(Instant.parse(node.get(TIMESTAMP).asText()));
        }
        if (node.has(EVENT)) {
            keyValueEvent.setEvent(node.get(EVENT).asText());
        }
        if (node.has(TYPE)) {
            keyValueEvent.setType(KeyType.valueOf(node.get(TYPE).asText()));
        }
        if (node.has(OPERATION)) {
            keyValueEvent.setOperation(KeyOperation.valueOf(node.get(OPERATION).asText()));
        }
        if (node.has(TTL)) {
            keyValueEvent.setTtl(Instant.parse(node.get(TTL).asText()));
        }
        if (node.has(VALUE)) {
            keyValueEvent.setValue(value(keyValueEvent.getType(), node.get(VALUE), ctxt));
        }
        return keyValueEvent;
    }

    private Object value(KeyType keyType, JsonNode node, DeserializationContext ctxt) throws IOException {
        switch (keyType) {
            case stream:
                return streamMessages((ArrayNode) node, ctxt);
            case zset:
                return scoredValues((ArrayNode) node);
            case timeseries:
                return samples((ArrayNode) node);
            case hash:
                return ctxt.readTreeAsValue(node, Map.class);
            case string:
            case json:
                return node.asText();
            case list:
                return ctxt.readTreeAsValue(node, Collection.class);
            case set:
                return ctxt.readTreeAsValue(node, Set.class);
            default:
                return null;
        }
    }

    private Collection<Sample> samples(ArrayNode node) {
        Collection<Sample> samples = new ArrayList<>(node.size());
        for (int index = 0; index < node.size(); index++) {
            JsonNode sampleNode = node.get(index);
            if (sampleNode != null) {
                samples.add(sample(sampleNode));
            }
        }
        return samples;
    }

    private Sample sample(JsonNode node) {
        LongNode timestampNode = (LongNode) node.get(TIMESTAMP);
        long timestamp = timestampNode == null || timestampNode.isNull() ? 0 : timestampNode.asLong();
        DoubleNode valueNode = (DoubleNode) node.get(VALUE);
        double value = valueNode == null || valueNode.isNull() ? 0 : valueNode.asDouble();
        return Sample.of(timestamp, value);
    }

    private Set<ScoredValue<String>> scoredValues(ArrayNode node) {
        Set<ScoredValue<String>> scoredValues = new HashSet<>(node.size());
        for (int index = 0; index < node.size(); index++) {
            JsonNode scoredValueNode = node.get(index);
            if (scoredValueNode != null) {
                scoredValues.add(scoredValue(scoredValueNode));
            }
        }
        return scoredValues;
    }

    private ScoredValue<String> scoredValue(JsonNode scoredValueNode) {
        JsonNode valueNode = scoredValueNode.get(VALUE);
        String value = valueNode == null || valueNode.isNull() ? null : valueNode.asText();
        JsonNode scoreNode = scoredValueNode.get(SCORE);
        double score = scoreNode == null || scoreNode.isNull() ? 0 : scoreNode.asDouble();
        return ScoredValue.just(score, value);
    }

    private Collection<StreamMessage<String, String>> streamMessages(ArrayNode node, DeserializationContext ctxt)
            throws IOException {
        Collection<StreamMessage<String, String>> messages = new ArrayList<>(node.size());
        for (int index = 0; index < node.size(); index++) {
            JsonNode messageNode = node.get(index);
            if (messageNode != null) {
                messages.add(streamMessage(messageNode, ctxt));
            }
        }
        return messages;
    }

    @SuppressWarnings("unchecked")
    private StreamMessage<String, String> streamMessage(JsonNode messageNode, DeserializationContext ctxt) throws IOException {
        JsonNode streamNode = messageNode.get(STREAM);
        String stream = streamNode == null || streamNode.isNull() ? null : streamNode.asText();
        JsonNode bodyNode = messageNode.get(BODY);
        Map<String, String> body = ctxt.readTreeAsValue(bodyNode, Map.class);
        JsonNode idNode = messageNode.get(ID);
        String id = idNode == null || idNode.isNull() ? null : idNode.asText();
        return new StreamMessage<>(stream, id, body);
    }

}
