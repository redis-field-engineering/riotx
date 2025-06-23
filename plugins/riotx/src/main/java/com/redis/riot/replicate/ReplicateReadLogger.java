package com.redis.riot.replicate;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyValueEvent;
import io.lettuce.core.codec.RedisCodec;
import org.slf4j.Logger;
import org.springframework.batch.core.ItemReadListener;

import java.util.function.Function;

public class ReplicateReadLogger<K> implements ItemReadListener<KeyValueEvent<K>> {

    private final Logger logger;

    private final Function<K, String> toString;

    public ReplicateReadLogger(Logger logger, RedisCodec<K, ?> codec) {
        this.logger = logger;
        this.toString = BatchUtils.toStringKeyFunction(codec);
    }

    @Override
    public void afterRead(KeyValueEvent<K> item) {
        if (logger.isInfoEnabled()) {
            logger.info("Key {}", toString.apply(item.getKey()));
        }
    }

}
