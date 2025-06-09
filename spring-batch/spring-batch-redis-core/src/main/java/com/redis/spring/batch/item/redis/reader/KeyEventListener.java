package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyEvent;

public interface KeyEventListener<K> {

    void onKeyEvent(KeyEvent<K> keyEvent);

}
