package com.redis.riot.rdi;

import java.util.Map;

public class ChangeEvent {

    private Map<String, Object> key;

    private ChangeEventValue value;

    public Map<String, Object> getKey() {
        return key;
    }

    public void setKey(Map<String, Object> key) {
        this.key = key;
    }

    public ChangeEventValue getValue() {
        return value;
    }

    public void setValue(ChangeEventValue value) {
        this.value = value;
    }

}
