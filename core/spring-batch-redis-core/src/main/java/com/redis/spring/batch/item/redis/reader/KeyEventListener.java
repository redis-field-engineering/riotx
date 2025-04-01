package com.redis.spring.batch.item.redis.reader;

public interface KeyEventListener<K> {

	void keyEvent(KeyEvent<K> keyEvent);

}
