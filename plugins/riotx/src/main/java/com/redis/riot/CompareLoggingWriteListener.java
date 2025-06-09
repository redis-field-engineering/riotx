package com.redis.riot;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import com.redis.batch.BatchUtils;
import com.redis.spring.batch.item.redis.reader.KeyComparison;

import io.lettuce.core.codec.RedisCodec;

public class CompareLoggingWriteListener<K> implements ItemWriteListener<KeyComparison<K>> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Function<K, String> toStringKeyFunction;

    public CompareLoggingWriteListener(RedisCodec<K, ?> codec) {
        toStringKeyFunction = BatchUtils.toStringKeyFunction(codec);
    }

    @Override
    public void afterWrite(Chunk<? extends KeyComparison<K>> items) {
        items.forEach(this::write);
    }

    private void write(KeyComparison<K> item) {
        switch (item.getStatus()) {
            case MISSING:
                log.error("Missing {} {}", item.getSource().getType(), key(item));
                break;
            case TYPE:
                log.error("Type mismatch on key {}. Expected {} but was {}", key(item), item.getSource().getType(),
                        item.getTarget().getType());
                break;
            case VALUE:
                log.error("Value mismatch on {} {}", item.getSource().getType(), key(item));
                break;
            case TTL:
                log.error("TTL mismatch on key {}. Expected {} but was {}", key(item), item.getSource().getTtl(),
                        item.getTarget().getTtl());
                break;
            default:
                break;
        }
    }

    private String key(KeyComparison<K> comparison) {
        return toStringKeyFunction.apply(comparison.getSource().getKey());
    }

}
