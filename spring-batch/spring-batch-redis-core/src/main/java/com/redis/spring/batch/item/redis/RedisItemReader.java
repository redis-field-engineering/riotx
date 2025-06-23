package com.redis.spring.batch.item.redis;

import com.redis.batch.KeyEvent;
import com.redis.batch.OperationExecutor;
import com.redis.batch.RedisBatchOperation;
import com.redis.batch.operation.KeyValueReadOperation;
import com.redis.spring.batch.item.AbstractCountingItemReader;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.codec.RedisCodec;
import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class RedisItemReader<K, V, T> extends AbstractCountingItemReader<T> {

    public static final int DEFAULT_POOL_SIZE = OperationExecutor.DEFAULT_POOL_SIZE;

    public static final int DEFAULT_BATCH_SIZE = 50;

    protected final RedisCodec<K, V> codec;

    private final RedisBatchOperation<K, V, KeyEvent<K>, T> operation;

    private int poolSize = DEFAULT_POOL_SIZE;

    protected ReadFrom readFrom;

    protected int batchSize = DEFAULT_BATCH_SIZE;

    protected String keyPattern;

    protected String keyType;

    protected AbstractRedisClient client;

    private OperationExecutor<K, V, KeyEvent<K>, T> operationExecutor;

    private Predicate<KeyEvent<K>> keyEventFilter = t -> true;

    protected RedisItemReader(RedisCodec<K, V> codec, RedisBatchOperation<K, V, KeyEvent<K>, T> operation) {
        this.codec = codec;
        this.operation = operation;
    }

    public List<T> read(List<? extends KeyEvent<K>> keyEvents) throws Exception {
        return operationExecutor.execute(keyEvents.stream().filter(keyEventFilter).collect(Collectors.toList()));
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

    public RedisBatchOperation<K, V, KeyEvent<K>, T> getOperation() {
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

    public void setMemoryLimit(DataSize limit) {
        Assert.notNull(limit, "Limit must not be null");
        if (operation instanceof KeyValueReadOperation) {
            ((KeyValueReadOperation) operation).setLimit(limit.toBytes());
        }
    }

    public void setMemoryUsageSamples(int samples) {
        if (operation instanceof KeyValueReadOperation) {
            ((KeyValueReadOperation) operation).setSamples(samples);
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

    public Predicate<KeyEvent<K>> getKeyEventFilter() {
        return keyEventFilter;
    }

    public void setKeyEventFilter(Predicate<KeyEvent<K>> predicate) {
        this.keyEventFilter = predicate;
    }

}
