package com.redis.spring.batch.item.redis.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.OperationExecutor;

public class KeyComparisonItemWriter<K, V> extends AbstractItemStreamItemWriter<KeyValue<K>> {

	public static final String EVENT = "COMPARE";

	private final RedisItemReader<K, V> targetReader;
	private final KeyComparator<K> comparator;
	private final KeyComparisonStats stats = new KeyComparisonStats();
	private final List<KeyComparisonListener<K>> listeners = new ArrayList<>();

	private OperationExecutor<K, V, KeyEvent<K>, KeyValue<K>> targetOperationExecutor;

	public KeyComparisonItemWriter(RedisItemReader<K, V> targetReader) {
		this(targetReader, new DefaultKeyComparator<>(targetReader.getCodec()));
	}

	public KeyComparisonItemWriter(RedisItemReader<K, V> targetReader, KeyComparator<K> comparator) {
		this.targetReader = targetReader;
		this.comparator = comparator;
		setName(ClassUtils.getShortName(getClass()));
	}

	public void addListener(KeyComparisonListener<K> listener) {
		this.listeners.add(listener);
	}

	@Override
	public void setName(String name) {
		targetReader.setName(name + "-target-reader");
		super.setName(name);
	}

	@Override
	public void write(Chunk<? extends KeyValue<K>> items) throws Exception {
		Chunk<KeyValue<K>> targetItems = targetOperationExecutor
				.process(new Chunk<>(items.getItems().stream().map(this::process).collect(Collectors.toList())));
		Iterator<? extends KeyValue<K>> iterator = items.iterator();
		Iterator<KeyValue<K>> targetIterator = targetItems == null ? Collections.emptyIterator()
				: targetItems.iterator();
		while (iterator.hasNext()) {
			KeyValue<K> source = iterator.next();
			KeyValue<K> target = targetIterator.hasNext() ? targetIterator.next() : null;
			KeyComparison<K> comparison = comparator.compare(source, target);
			stats.add(comparison);
			listeners.forEach(l -> l.comparison(comparison));
		}
	}

	private KeyEvent<K> process(KeyValue<K> item) {
		KeyEvent<K> processedItem = new KeyEvent<>();
		processedItem.setKey(item.getKey());
		processedItem.setEvent(EVENT);
		processedItem.setTimestamp(item.getTimestamp());
		return processedItem;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (targetOperationExecutor == null) {
			targetOperationExecutor = targetReader.operationExecutor();
			targetOperationExecutor.open(executionContext);
		}
		super.open(executionContext);
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		targetOperationExecutor.update(executionContext);
		super.update(executionContext);
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		super.close();
		if (targetOperationExecutor != null) {
			targetOperationExecutor.close();
			targetOperationExecutor = null;
		}
	}

	public RedisItemReader<K, V> getTargetReader() {
		return targetReader;
	}

	public KeyComparisonStats getStats() {
		return stats;
	}

}
