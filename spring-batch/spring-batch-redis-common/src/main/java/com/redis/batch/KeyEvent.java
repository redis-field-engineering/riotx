package com.redis.batch;

import java.time.Instant;

public class KeyEvent<K> {

    public static final String EVENT_DEL = "del";

    private K key;

    private Instant timestamp;

    private String event;

    private KeyOperation operation;

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public KeyOperation getOperation() {
        return operation;
    }

    public void setOperation(KeyOperation operation) {
        this.operation = operation;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        KeyEvent<?> keyEvent = (KeyEvent<?>) o;
        return BatchUtils.keyEquals(key, keyEvent.key);
    }

    @Override
    public int hashCode() {
        return BatchUtils.keyHashCode(key);
    }

    public static KeyType type(String event) {
        if (event.startsWith("xgroup-")) {
            return KeyType.stream;
        }
        if (event.startsWith("ts.")) {
            return KeyType.timeseries;
        }
        if (event.startsWith("json.")) {
            return KeyType.json;
        }
        switch (event) {
            case "set":
            case "setrange":
            case "incrby":
            case "incrbyfloat":
            case "append":
                return KeyType.string;
            case "lpush":
            case "rpush":
            case "rpop":
            case "lpop":
            case "linsert":
            case "lset":
            case "lrem":
            case "ltrim":
                return KeyType.list;
            case "hset":
            case "hincrby":
            case "hincrbyfloat":
            case "hdel":
                return KeyType.hash;
            case "sadd":
            case "spop":
            case "sinterstore":
            case "sunionstore":
            case "sdiffstore":
                return KeyType.set;
            case "zincr":
            case "zadd":
            case "zrem":
            case "zrembyscore":
            case "zrembyrank":
            case "zdiffstore":
            case "zinterstore":
            case "zunionstore":
                return KeyType.zset;
            case "xadd":
            case "xtrim":
            case "xdel":
            case "xsetid":
                return KeyType.stream;
            case EVENT_DEL:
                return KeyType.none;
            default:
                return KeyType.unknown;
        }
    }

    public KeyType type() {
        return type(event);
    }

}
