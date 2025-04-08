package com.redis.spring.batch.item.redis.reader.pubsub;

public interface PubSubMessageListener<K, V> {

    void onMessage(PubSubMessage<K, V> message);

}
