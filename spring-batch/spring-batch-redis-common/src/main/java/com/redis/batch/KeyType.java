package com.redis.batch;

import java.util.HashMap;
import java.util.Map;

public enum KeyType {

    NONE("none"), HASH("hash"), JSON("ReJSON-RL"), LIST("list"), SET("set"), STREAM("stream"), STRING("string"), TIMESERIES(
            "TSDB-TYPE"), ZSET("zset");

    private final String string;

    KeyType(String string) {
        this.string = string;
    }

    public String getString() {
        return string;
    }

    private static final Map<String, KeyType> typeMap = typeMap();

    private static Map<String, KeyType> typeMap() {
        Map<String, KeyType> map = new HashMap<>();
        for (KeyType keyType : KeyType.values()) {
            map.put(keyType.getString().toLowerCase(), keyType);
        }
        return map;
    }

    public static KeyType of(String type) {
        if (type == null) {
            return null;
        }
        return typeMap.get(type.toLowerCase());
    }
}
