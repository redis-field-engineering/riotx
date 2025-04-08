package com.redis.spring.batch.item.redis.reader.pubsub;

import org.springframework.context.SmartLifecycle;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public interface PubSubListenerContainer<K, V> extends SmartLifecycle {

    Subscription receive(K pattern, PubSubMessageListener<K, V> listener);

    static <K, V> PubSubListenerContainer<K, V> create(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new DefaultPubSubListenerContainer<>(client, codec);
    }

}
