package com.redis.spring.batch.item.redis.reader.pubsub;

public interface Subscription {

    boolean isActive();

    void cancel();

}
