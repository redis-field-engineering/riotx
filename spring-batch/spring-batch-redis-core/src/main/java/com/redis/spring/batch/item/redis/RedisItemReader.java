package com.redis.spring.batch.item.redis;

import com.redis.spring.batch.item.AbstractCountingItemReader;
import com.redis.batch.KeyValue;
import com.redis.batch.OperationExecutor;
import com.redis.batch.RedisOperation;
import com.redis.batch.operation.KeyValueRead;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.springframework.data.util.Predicates;
import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class RedisItemReader<K, V> extends AbstractCountingItemReader<KeyValue<K>> {

    public static final int DEFAULT_POOL_SIZE = OperationExecutor.DEFAULT_POOL_SIZE;

    public static final int DEFAULT_BATCH_SIZE = 50;

    protected final RedisCodec<K, V> codec;

    private final RedisOperation<K, V, K, KeyValue<K>> operation;

    private int poolSize = DEFAULT_POOL_SIZE;

    protected ReadFrom readFrom;

    protected int batchSize = DEFAULT_BATCH_SIZE;

    protected String keyPattern;

    protected String keyType;

    protected MeterRegistry meterRegistry = Metrics.globalRegistry;

    protected AbstractRedisClient client;

    private OperationExecutor<K, V, K, KeyValue<K>> operationExecutor;

    private Predicate<K> keyFilter = Predicates.isTrue();

    protected RedisItemReader(RedisCodec<K, V> codec, RedisOperation<K, V, K, KeyValue<K>> operation) {
        this.codec = codec;
        this.operation = operation;
    }

    public List<KeyValue<K>> read(List<? extends KeyValue<K>> keyEvents) throws Exception {
        List<? extends K> keys = keyEvents.stream().map(KeyValue::getKey).collect(Collectors.toList());
        List<KeyValue<K>> keyValues = operationExecutor.execute(keys);
        for (int index = 0; index < keyEvents.size(); index++) {
            KeyValue<K> keyEvent = keyEvents.get(index);
            KeyValue<K> keyValue = keyValues.get(index);
            keyValue.setEvent(keyEvent.getEvent());
            keyValue.setTime(keyEvent.getTime());
        }
        return keyValues;
    }

    @Override
    protected synchronized void doOpen() throws Exception {
        if (operationExecutor == null) {
            Assert.notNull(client, getName() + ": Redis client not set");
            operationExecutor = new OperationExecutor<>(codec, operation);
            operationExecutor.setClient(client);
            operationExecutor.setPoolSize(poolSize);
            operationExecutor.setReadFrom(readFrom);
            operationExecutor.afterPropertiesSet();
        }
    }

    @Override
    protected void doClose() {
        if (operationExecutor != null) {
            operationExecutor.close();
            operationExecutor = null;
        }
    }

    protected boolean acceptKeyType(String type) {
        return keyType == null || keyType.equalsIgnoreCase(type);
    }

    protected boolean acceptKey(K key) {
        return keyFilter.test(key);
    }

    public RedisOperation<K, V, K, KeyValue<K>> getOperation() {
        return operation;
    }

    public RedisCodec<K, V> getCodec() {
        return codec;
    }

    public AbstractRedisClient getClient() {
        return client;
    }

    public void setClient(AbstractRedisClient client) {
        this.client = client;
    }

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public void setMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int size) {
        this.poolSize = size;
    }

    public ReadFrom getReadFrom() {
        return readFrom;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int size) {
        this.batchSize = size;
    }

    public void setMemoryLimit(long limit) {
        setMemoryLimit(DataSize.ofBytes(limit));
    }

    public void setMemoryLimit(DataSize limit) {
        Assert.notNull(limit, "Limit must not be null");
        if (operation instanceof KeyValueRead) {
            ((KeyValueRead<K, V>) operation).limit(limit.toBytes());
        }
    }

    public void setMemoryUsageSamples(int samples) {
        if (operation instanceof KeyValueRead) {
            ((KeyValueRead<K, V>) operation).memoryUsageSamples(samples);
        }
    }

    public String getKeyPattern() {
        return keyPattern;
    }

    public void setKeyPattern(String pattern) {
        this.keyPattern = pattern;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }

    public Predicate<K> getKeyFilter() {
        return keyFilter;
    }

    public void setKeyFilter(Predicate<K> filter) {
        this.keyFilter = filter;
    }

}
