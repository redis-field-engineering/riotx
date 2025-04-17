package com.redis.riotx;

import java.util.function.Function;

import org.slf4j.Logger;
import org.springframework.batch.core.ItemReadListener;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class ReplicateReadLogger<K> implements ItemReadListener<KeyValue<K>> {

    private final Logger logger;

    private final Function<K, String> toString;

    public ReplicateReadLogger(Logger logger, RedisCodec<K, ?> codec) {
        this.logger = logger;
        this.toString = BatchUtils.toStringKeyFunction(codec);
    }

    @Override
    public void afterRead(KeyValue<K> item) {
        if (logger.isInfoEnabled()) {
            logger.info("Key {}", toString.apply(item.getKey()));
        }
    }

}
