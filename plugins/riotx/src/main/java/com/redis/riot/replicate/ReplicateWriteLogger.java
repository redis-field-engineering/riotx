package com.redis.riot.replicate;

import com.redis.batch.BatchUtils;
import com.redis.batch.KeyTtlTypeEvent;
import io.lettuce.core.codec.RedisCodec;
import org.slf4j.Logger;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import java.util.function.Function;

public class ReplicateWriteLogger<K> implements ItemWriteListener<KeyTtlTypeEvent<K>> {

    private final Logger logger;

    private final Function<K, String> toString;

    public ReplicateWriteLogger(Logger logger, RedisCodec<K, ?> codec) {
        this.logger = logger;
        this.toString = BatchUtils.toStringKeyFunction(codec);
    }

    protected void log(String message, Chunk<? extends KeyTtlTypeEvent<K>> items) {
        if (logger.isInfoEnabled()) {
            for (KeyTtlTypeEvent<K> item : items) {
                logger.info(message, string(item));
            }
        }
    }

    protected String string(KeyTtlTypeEvent<K> item) {
        return toString.apply(item.getKey());
    }

    @Override
    public void beforeWrite(Chunk<? extends KeyTtlTypeEvent<K>> items) {
        log("Writing {}", items);
    }

    @Override
    public void afterWrite(Chunk<? extends KeyTtlTypeEvent<K>> items) {
        log("Wrote {}", items);
    }

    @Override
    public void onWriteError(Exception exception, Chunk<? extends KeyTtlTypeEvent<K>> items) {
        if (logger.isErrorEnabled()) {
            for (KeyTtlTypeEvent<K> item : items) {
                logger.error("Could not write {}", string(item), exception);
            }
        }
    }

}
