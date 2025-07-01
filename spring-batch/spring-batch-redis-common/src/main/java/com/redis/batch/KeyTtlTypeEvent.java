package com.redis.batch;

import java.time.Instant;

public class KeyTtlTypeEvent<K> extends KeyEvent<K> {

    private Instant ttl;

    private String typeString;

    private KeyType type;

    /**
     * @return expiration time for the key, or null if none set
     */
    public Instant getTtl() {
        return ttl;
    }

    public void setTtl(Instant ttl) {
        this.ttl = ttl;
    }

    public String getTypeString() {
        return typeString;
    }

    public void setTypeString(String typeString) {
        this.typeString = typeString;
    }

    public KeyType getType() {
        return type;
    }

    public void setType(KeyType type) {
        this.type = type;
    }

}
