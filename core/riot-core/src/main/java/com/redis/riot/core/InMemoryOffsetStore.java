package com.redis.riot.core;

import java.util.Map;

public class InMemoryOffsetStore implements OffsetStore {

    private Map<String, Object> offset;

    @Override
    public void store(Map<String, Object> offset) {
        this.offset = offset;
    }

    @Override
    public Map<String, Object> getOffset() {
        return offset;
    }

}
