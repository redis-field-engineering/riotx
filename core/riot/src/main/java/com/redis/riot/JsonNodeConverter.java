package com.redis.riot;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonNodeConverter {
    public static Object convertJsonNode(JsonNode jsonNode) {
        if (jsonNode.isValueNode()) {
            if (jsonNode.isTextual()) {
                return jsonNode.asText();
            }
            if (jsonNode.isNumber()) {
                return jsonNode.numberValue();
            }
            if (jsonNode.isBoolean()) {
                return jsonNode.asBoolean();
            }
            if (jsonNode.isNull()) {
                return null;
            }
        } else if (jsonNode.isArray()) {
            List<Object> list = new ArrayList<>();
            jsonNode.forEach(element -> list.add(convertJsonNode(element)));
            return list;
        } else if (jsonNode.isObject()) {
            Map<String, Object> map = new HashMap<>();
            jsonNode.fields().forEachRemaining(entry -> map.put(entry.getKey(), convertJsonNode(entry.getValue())));
            return map;
        }
        throw new IllegalArgumentException("Unsupported JsonNode type: " + jsonNode.getNodeType());
    }
}
