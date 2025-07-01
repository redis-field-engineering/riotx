package com.redis.batch.operation;

import com.redis.batch.KeyDumpEvent;
import io.lettuce.core.codec.ByteArrayCodec;

public class KeyDumpRead extends AbstractKeyValueRead<byte[], byte[], KeyDumpEvent<byte[]>> {

    public KeyDumpRead() {
        super(ByteArrayCodec.INSTANCE, Mode.dump);
    }

    @Override
    protected void setValue(KeyDumpEvent<byte[]> keyValue, Object value) {
        keyValue.setDump((byte[]) value);
    }

    @Override
    protected KeyDumpEvent<byte[]> keyValueEvent() {
        return new KeyDumpEvent<>();
    }

}
