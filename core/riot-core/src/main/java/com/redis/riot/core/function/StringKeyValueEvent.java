package com.redis.riot.core.function;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyValueEvent;
import io.lettuce.core.codec.RedisCodec;

import java.util.function.Function;

public class StringKeyValueEvent<K> implements Function<KeyValueEvent<String>, KeyValueEvent<K>> {

    private final Function<String, K> stringKeyFunction;

    public StringKeyValueEvent(RedisCodec<K, ?> codec) {
        this.stringKeyFunction = BatchUtils.stringKeyFunction(codec);
    }

    @Override
    public KeyValueEvent<K> apply(KeyValueEvent<String> item) {
        return KeyValueEvent.of(stringKeyFunction.apply(item.getKey()), item);
    }

}
