package com.redis.spring.batch.item.redis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.spring.batch.UniqueBlockingQueue;
import com.redis.batch.Key;
import com.redis.batch.KeyValue;
import com.redis.spring.batch.item.redis.reader.DefaultKeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.StringCodec;

class KeyTests {

	@Test
	void keySet() {
		Set<Key<byte[]>> set = new HashSet<>();
		set.add(new Key<>(new byte[] { 1, 2, 3 }));
		set.add(new Key<>(new byte[] { 1, 2, 3 }));
		Assertions.assertEquals(1, set.size());
		set.add(new Key<>(new byte[] { 1, 2 }));
		Assertions.assertEquals(2, set.size());
	}
	
	@Test
	void compareStreamMessageId() {
		DefaultKeyComparator<String, String> comparator = new DefaultKeyComparator<>(StringCodec.UTF8);
		String key = "key:1";
		String type = "stream";
		String messageId = "12345";
		Map<String, String> body = new HashMap<>();
		KeyValue<String> kv1 = new KeyValue<>();
		kv1.setKey(key);
		kv1.setType(type);
		kv1.setValue(Arrays.asList(new StreamMessage<>(key, messageId, body)));
		KeyValue<String> kv2 = new KeyValue<>();
		kv2.setKey(key);
		kv2.setType(type);
		StreamMessage<String, String> message2 = new StreamMessage<>(key, messageId + "1", body);
		kv2.setValue(Arrays.asList(message2));
		Assertions.assertEquals(Status.VALUE, comparator.compare(kv1, kv2).getStatus());
		comparator.setStreamMessageIds(false);
		Assertions.assertEquals(Status.OK, comparator.compare(kv1, kv2).getStatus());
	}
	
	@Test
	void uniqueQueueByteArray() throws InterruptedException {
		UniqueBlockingQueue<Key<byte[]>> queue = new UniqueBlockingQueue<>();
		queue.offer(new Key<>(new byte[] { 1, 2, 3 }));
		queue.offer(new Key<>(new byte[] { 3, 4, 5 }));
		Assertions.assertEquals(2, queue.size());
		queue.offer(new Key<>(new byte[] { 1, 2, 3 }));
		Assertions.assertEquals(2, queue.size());
		Assertions.assertArrayEquals(new byte[] { 1, 2, 3 }, queue.poll().getKey());
		Assertions.assertEquals(1, queue.size());
		Assertions.assertArrayEquals(new byte[] { 3, 4, 5 }, queue.take().getKey());
		Assertions.assertTrue(queue.isEmpty());
	}

}
