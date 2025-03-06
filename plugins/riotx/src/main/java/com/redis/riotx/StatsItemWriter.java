package com.redis.riotx;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redis.spring.batch.item.redis.common.KeyValue;

public class StatsItemWriter extends AbstractItemStreamItemWriter<KeyValue<String>> {

	public static final int DEFAULT_MAX_SIZE = 10;

	private int maxSize = DEFAULT_MAX_SIZE;
	private PriorityQueue<KeyValue<String>> queue;

	/**
	 * 
	 * @return Top N KeyValues in descending memory-usage order
	 */
	public List<KeyValue<String>> getTopN() {
		// Already in descending order due to max heap
		return new ArrayList<>(queue);
	}

	/**
	 * 
	 * @param kv1 a KeyValue
	 * @param kv2 another KeyValue
	 * @return 0 if kv1 and kv2 have the same memory usage, -1
	 */
	public int compareMemoryUsage(KeyValue<String> kv1, KeyValue<String> kv2) {
		return Long.compare(kv2.getMemoryUsage(), kv1.getMemoryUsage());
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (queue == null) {
			// Max heap based on memory usage - largest at top
			queue = new PriorityQueue<>(this::compareMemoryUsage);
		}
		super.open(executionContext);
	}

	@Override
	public synchronized void write(Chunk<? extends KeyValue<String>> chunk) throws Exception {
		for (KeyValue<String> keyValue : chunk) {
			add(keyValue);
		}
	}

	/**
	 * Adds a KeyValue to be considered for top-N by memory usage. Not thread-safe.
	 * 
	 * @param keyValue the KeyValue to consider for top N
	 */
	public void add(KeyValue<String> keyValue) {
		if (queue.size() < maxSize) {
			queue.offer(keyValue);
		} else if (keyValue.getMemoryUsage() > queue.peek().getMemoryUsage()) {
			queue.poll(); // Remove largest element
			queue.offer(keyValue);
		}
	}

}
