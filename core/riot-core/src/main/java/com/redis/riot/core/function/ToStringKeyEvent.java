package com.redis.riot.core.function;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyEvent;
import io.lettuce.core.codec.RedisCodec;

import java.util.function.Function;

public class ToStringKeyEvent<K> implements Function<KeyEvent<K>, KeyEvent<String>> {

    private final Function<K, String> toStringKeyFunction;

    public ToStringKeyEvent(RedisCodec<K, ?> codec) {
        this.toStringKeyFunction = BatchUtils.toStringKeyFunction(codec);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public KeyEvent<String> apply(KeyEvent<K> item) {
        KeyEvent<String> result = (KeyEvent) item;
        result.setKey(toStringKeyFunction.apply(item.getKey()));
        return result;
    }

}
