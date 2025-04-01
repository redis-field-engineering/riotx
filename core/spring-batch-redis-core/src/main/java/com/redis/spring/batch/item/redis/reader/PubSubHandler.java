package com.redis.spring.batch.item.redis.reader;

import java.util.function.BiConsumer;

import org.springframework.batch.item.ItemStream;

public interface PubSubHandler<K, V> extends ItemStream {

	void addConsumer(BiConsumer<K, V> consumer);

}
