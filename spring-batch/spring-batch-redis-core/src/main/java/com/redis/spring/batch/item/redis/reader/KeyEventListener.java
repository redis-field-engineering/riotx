package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyValue;

public interface KeyEventListener<K> {

    void onKeyEvent(KeyValue<K> keyEvent);

}
