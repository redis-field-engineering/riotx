package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyStructEvent;
import com.redis.spring.batch.item.AbstractCountingItemReader;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class KeyComparisonItemReader<K, V> extends AbstractCountingItemReader<KeyComparison<K>> {

    public static final int DEFAULT_BATCH_SIZE = 50;

    private final RedisScanItemReader<K, V, KeyStructEvent<K, V>> sourceReader;

    private final RedisScanItemReader<K, V, KeyStructEvent<K, V>> targetReader;

    private ItemProcessor<KeyStructEvent<K, V>, KeyStructEvent<K, V>> processor;

    private int batchSize = DEFAULT_BATCH_SIZE;

    private KeyComparator<K, KeyStructEvent<K, V>> comparator;

    private Iterator<KeyComparison<K>> iterator = Collections.emptyIterator();

    public KeyComparisonItemReader(RedisScanItemReader<K, V, KeyStructEvent<K, V>> sourceReader,
            RedisScanItemReader<K, V, KeyStructEvent<K, V>> targetReader) {
        this.sourceReader = sourceReader;
        this.targetReader = targetReader;
        this.comparator = new KeyStructComparator<>(sourceReader.getCodec());
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        sourceReader.open(executionContext);
        targetReader.open(executionContext);
        super.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        sourceReader.update(executionContext);
        targetReader.update(executionContext);
        super.update(executionContext);
    }

    @Override
    protected void doOpen() {
        // Do nothing
    }

    @Override
    public synchronized void close() throws ItemStreamException {
        super.close();
        targetReader.close();
        sourceReader.close();
    }

    @Override
    protected void doClose() {
        // Do nothing
    }

    @Override
    protected synchronized KeyComparison<K> doRead() throws Exception {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        List<KeyStructEvent<K, V>> sourceValues = new ArrayList<>();
        KeyStructEvent<K, V> sourceValue;
        while (sourceValues.size() < batchSize && (sourceValue = sourceReader.read()) != null) {
            KeyStructEvent<K, V> processedKeyValueEvent = process(sourceValue);
            if (processedKeyValueEvent != null) {
                sourceValues.add(processedKeyValueEvent);
            }
        }
        List<KeyStructEvent<K, V>> targetValues = targetReader.read(sourceValues);
        Assert.isTrue(targetValues.size() == sourceValues.size(), "Size mismatch between target and source values");
        List<KeyComparison<K>> comparisons = new ArrayList<>();
        for (int index = 0; index < sourceValues.size(); index++) {
            comparisons.add(comparator.compare(sourceValues.get(index), targetValues.get(index)));
        }
        iterator = comparisons.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    private KeyStructEvent<K, V> process(KeyStructEvent<K, V> event) throws Exception {
        if (processor == null) {
            return event;
        }
        return processor.process(event);
    }

    public RedisScanItemReader<K, V, KeyStructEvent<K, V>> getSourceReader() {
        return sourceReader;
    }

    public RedisScanItemReader<K, V, KeyStructEvent<K, V>> getTargetReader() {
        return targetReader;
    }

    public KeyComparator<K, KeyStructEvent<K, V>> getComparator() {
        return comparator;
    }

    public void setComparator(KeyComparator<K, KeyStructEvent<K, V>> comparator) {
        this.comparator = comparator;
    }

    public ItemProcessor<KeyStructEvent<K, V>, KeyStructEvent<K, V>> getProcessor() {
        return processor;
    }

    public void setProcessor(ItemProcessor<KeyStructEvent<K, V>, KeyStructEvent<K, V>> processor) {
        this.processor = processor;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

}
