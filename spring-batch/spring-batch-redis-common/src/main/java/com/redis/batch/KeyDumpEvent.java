package com.redis.batch;

public class KeyDumpEvent<K> extends KeyTtlTypeEvent<K> {

    private byte[] dump;

    public byte[] getDump() {
        return dump;
    }

    public void setDump(byte[] dump) {
        this.dump = dump;
    }

}
