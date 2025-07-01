package com.redis.batch.operation;

import com.redis.batch.KeyStructEvent;
import io.lettuce.core.codec.RedisCodec;

public class KeyNoneRead<K, V> extends AbstractKeyValueRead<K, V, KeyStructEvent<K, V>> {

    public KeyNoneRead(RedisCodec<K, V> codec) {
        super(codec, Mode.none);
    }

    @Override
    protected KeyStructEvent<K, V> keyValueEvent() {
        return new KeyStructEvent<>();
    }

    @Override
    protected void setValue(KeyStructEvent<K, V> keyValue, Object value) {
        // ignore
    }

}
