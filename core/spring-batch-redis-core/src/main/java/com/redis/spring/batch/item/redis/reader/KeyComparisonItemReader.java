package com.redis.spring.batch.item.redis.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.item.redis.common.KeyValue;

public class KeyComparisonItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<KeyComparison<K>> {

    public static final int DEFAULT_BATCH_SIZE = 50;

    private final RedisScanItemReader<K, V> sourceReader;

    private final RedisScanItemReader<K, V> targetReader;

    private ItemProcessor<KeyValue<K>, KeyValue<K>> processor = new PassThroughItemProcessor<>();

    private int batchSize = DEFAULT_BATCH_SIZE;

    private KeyComparator<K> comparator;

    private Iterator<KeyComparison<K>> iterator = Collections.emptyIterator();

    public KeyComparisonItemReader(RedisScanItemReader<K, V> sourceReader, RedisScanItemReader<K, V> targetReader) {
        setName(ClassUtils.getShortName(getClass()));
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
    protected void doOpen() throws Exception {
        // Do nothing
    }

    @Override
    public synchronized void close() throws ItemStreamException {
        super.close();
        targetReader.close();
        sourceReader.close();
    }

    @Override
    protected void doClose() throws Exception {
        // Do nothing
    }

    @Override
    protected synchronized KeyComparison<K> doRead() throws Exception {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        List<KeyValue<K>> sourceValues = new ArrayList<>();
        KeyValue<K> sourceValue;
        while (sourceValues.size() < batchSize && (sourceValue = sourceReader.read()) != null) {
            KeyValue<K> processedKeyValue = processor.process(sourceValue);
            if (processedKeyValue != null) {
                sourceValues.add(processedKeyValue);
            }
        }
        List<KeyValue<K>> targetValues = targetReader.read(sourceValues);
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

    public KeyComparator<K> getComparator() {
        return comparator;
    }

    public void setComparator(KeyComparator<K> comparator) {
        this.comparator = comparator;
    }

    public ItemProcessor<KeyValue<K>, KeyValue<K>> getProcessor() {
        return processor;
    }

    public void setProcessor(ItemProcessor<KeyValue<K>, KeyValue<K>> processor) {
        this.processor = processor;
    }

}
