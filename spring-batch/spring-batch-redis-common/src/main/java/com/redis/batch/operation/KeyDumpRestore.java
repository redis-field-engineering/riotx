package com.redis.batch.operation;

import com.redis.batch.KeyDumpEvent;

public class KeyDumpRestore<K, V> extends Restore<K, V, KeyDumpEvent<K>> {

    public KeyDumpRestore() {
        super(KeyDumpEvent::getKey, KeyDumpEvent::getDump);
        setTtlFunction(KeyDumpEvent::getTtl);
    }

}
