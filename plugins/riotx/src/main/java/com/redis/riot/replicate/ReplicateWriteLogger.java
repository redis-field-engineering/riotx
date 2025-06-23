package com.redis.riot.replicate;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyValueEvent;
import io.lettuce.core.codec.RedisCodec;
import org.slf4j.Logger;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import java.util.function.Function;

public class ReplicateWriteLogger<K> implements ItemWriteListener<KeyValueEvent<K>> {

    private final Logger logger;

    private final Function<K, String> toString;

    public ReplicateWriteLogger(Logger logger, RedisCodec<K, ?> codec) {
        this.logger = logger;
        this.toString = BatchUtils.toStringKeyFunction(codec);
    }

    protected void log(String message, Chunk<? extends KeyValueEvent<K>> items) {
        if (logger.isInfoEnabled()) {
            for (KeyValueEvent<K> item : items) {
                logger.info(message, string(item));
            }
        }
    }

    protected String string(KeyValueEvent<K> item) {
        return toString.apply(item.getKey());
    }

    @Override
    public void beforeWrite(Chunk<? extends KeyValueEvent<K>> items) {
        log("Writing {}", items);
    }

    @Override
    public void afterWrite(Chunk<? extends KeyValueEvent<K>> items) {
        log("Wrote {}", items);
    }

    @Override
    public void onWriteError(Exception exception, Chunk<? extends KeyValueEvent<K>> items) {
        if (logger.isErrorEnabled()) {
            for (KeyValueEvent<K> item : items) {
                logger.error("Could not write {}", string(item), exception);
            }
        }
    }

}
