package com.redis.riot.core.function;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyEvent;
import io.lettuce.core.codec.RedisCodec;

import java.util.function.Function;

public class StringKeyEvent<K> implements Function<KeyEvent<String>, KeyEvent<K>> {

    private final Function<String, K> stringKeyFunction;

    public StringKeyEvent(RedisCodec<K, ?> codec) {
        this.stringKeyFunction = BatchUtils.stringKeyFunction(codec);
    }

    @SuppressWarnings("unchecked")
    @Override
    public KeyEvent<K> apply(KeyEvent<String> item) {
        KeyEvent<K> result = (KeyEvent<K>) item;
        result.setKey(stringKeyFunction.apply(item.getKey()));
        return result;
    }

}
