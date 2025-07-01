package com.redis.batch;

import java.util.HashMap;
import java.util.Map;

public enum KeyType {

    none("none"), hash("hash"), json("ReJSON-RL"), list("list"), set("set"), stream("stream"), string("string"), timeseries(
            "TSDB-TYPE"), zset("zset"), unknown("unknown");

    private final String name;

    KeyType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    private static final Map<String, KeyType> typeMap = typeMap();

    private static Map<String, KeyType> typeMap() {
        Map<String, KeyType> map = new HashMap<>();
        for (KeyType keyType : KeyType.values()) {
            map.put(keyType.getName().toLowerCase(), keyType);
        }
        return map;
    }

    public static KeyType of(String type) {
        if (type == null) {
            return unknown;
        }
        return typeMap.get(type.toLowerCase());
    }
}
