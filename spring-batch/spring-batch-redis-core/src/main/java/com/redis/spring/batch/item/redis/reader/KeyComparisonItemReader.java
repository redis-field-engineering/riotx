package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyValueEvent;
import com.redis.spring.batch.item.AbstractCountingItemReader;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class KeyComparisonItemReader<K, V, T> extends AbstractCountingItemReader<KeyComparison<K>> {

    public static final int DEFAULT_BATCH_SIZE = 50;

    private final RedisScanItemReader<K, V, KeyValueEvent<K>> sourceReader;

    private final RedisScanItemReader<K, V, KeyValueEvent<K>> targetReader;

    private ItemProcessor<KeyValueEvent<K>, KeyValueEvent<K>> processor;

    private int batchSize = DEFAULT_BATCH_SIZE;

    private KeyComparator<K> comparator;

    private Iterator<KeyComparison<K>> iterator = Collections.emptyIterator();

    public KeyComparisonItemReader(RedisScanItemReader<K, V, KeyValueEvent<K>> sourceReader,
            RedisScanItemReader<K, V, KeyValueEvent<K>> targetReader) {
        this.sourceReader = sourceReader;
        this.targetReader = targetReader;
        this.comparator = new DefaultKeyComparator<>(sourceReader.getCodec());
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
        List<KeyValueEvent<K>> sourceValues = new ArrayList<>();
        KeyValueEvent<K> sourceValue;
        while (sourceValues.size() < batchSize && (sourceValue = sourceReader.read()) != null) {
            KeyValueEvent<K> processedKeyValueEvent = process(sourceValue);
            if (processedKeyValueEvent != null) {
                sourceValues.add(processedKeyValueEvent);
            }
        }
        List<KeyValueEvent<K>> targetValues = targetReader.read(sourceValues);
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

    private KeyValueEvent<K> process(KeyValueEvent<K> keyValueEvent) throws Exception {
        if (processor == null) {
            return keyValueEvent;
        }
        return processor.process(keyValueEvent);
    }

    public RedisScanItemReader<K, V, KeyValueEvent<K>> getSourceReader() {
        return sourceReader;
    }

    public RedisScanItemReader<K, V, KeyValueEvent<K>> getTargetReader() {
        return targetReader;
    }

    public KeyComparator<K> getComparator() {
        return comparator;
    }

    public void setComparator(KeyComparator<K> comparator) {
        this.comparator = comparator;
    }

    public ItemProcessor<KeyValueEvent<K>, KeyValueEvent<K>> getProcessor() {
        return processor;
    }

    public void setProcessor(ItemProcessor<KeyValueEvent<K>, KeyValueEvent<K>> processor) {
        this.processor = processor;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

}
