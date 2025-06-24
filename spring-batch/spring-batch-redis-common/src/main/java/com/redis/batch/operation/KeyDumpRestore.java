package com.redis.batch.operation;

import com.redis.batch.KeyValueEvent;

public class KeyDumpRestore<K, V> extends Restore<K, V, KeyValueEvent<K>> {

    public KeyDumpRestore() {
        super(KeyValueEvent::getKey, event -> (byte[]) event.getValue());
        setTtlFunction(KeyValueEvent::getTtl);
    }

}
