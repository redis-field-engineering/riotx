package com.redis.batch.operation;

import io.lettuce.core.codec.ByteArrayCodec;

public class KeyDumpReadOperation extends KeyValueReadOperation<byte[], byte[]> {

    public KeyDumpReadOperation() {
        super(ByteArrayCodec.INSTANCE, Mode.DUMP);
    }

    @Override
    protected byte[] value(ReadResult<byte[]> result) {
        return (byte[]) result.getValue();
    }

}
