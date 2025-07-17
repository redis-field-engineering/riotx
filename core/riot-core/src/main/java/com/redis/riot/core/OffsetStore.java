package com.redis.riot.core;

import java.util.Map;

public interface OffsetStore {

    void clear();

    void store(Map<String, Object> offset) throws Exception;

    Map<String, Object> getOffset() throws Exception;

}
