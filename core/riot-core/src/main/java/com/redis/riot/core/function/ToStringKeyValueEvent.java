package com.redis.riot.core.function;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyValueEvent;
import io.lettuce.core.codec.RedisCodec;

import java.util.function.Function;

public class ToStringKeyValueEvent<K> implements Function<KeyValueEvent<K>, KeyValueEvent<String>> {

    private final Function<K, String> toStringKeyFunction;

    public ToStringKeyValueEvent(RedisCodec<K, ?> codec) {
        this.toStringKeyFunction = BatchUtils.toStringKeyFunction(codec);
    }

    @Override
    public KeyValueEvent<String> apply(KeyValueEvent<K> item) {
        return KeyValueEvent.of(toStringKeyFunction.apply(item.getKey()), item);
    }

}
