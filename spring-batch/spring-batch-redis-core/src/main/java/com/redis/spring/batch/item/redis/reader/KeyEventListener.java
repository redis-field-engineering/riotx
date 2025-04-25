package com.redis.spring.batch.item.redis.reader;

public interface KeyEventListener<K> {

    void onKeyEvent(KeyEvent<K> keyEvent);

}
