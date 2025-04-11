package com.redis.spring.batch.item.redis;

import java.util.List;
import java.util.function.Predicate;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.data.util.Predicates;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.OperationExecutor;
import com.redis.spring.batch.item.redis.common.RedisOperation;
import com.redis.spring.batch.item.redis.reader.KeyEvent;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;
import com.redis.spring.batch.item.redis.reader.MemoryUsage;
import com.redis.spring.batch.item.redis.reader.RedisLiveItemReader;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

public abstract class RedisItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<KeyValue<K>> {

    public static final int DEFAULT_POOL_SIZE = OperationExecutor.DEFAULT_POOL_SIZE;

    public static final int DEFAULT_BATCH_SIZE = 50;

    protected final RedisCodec<K, V> codec;

    private final RedisOperation<K, V, KeyEvent<K>, KeyValue<K>> operation;

    private int poolSize = DEFAULT_POOL_SIZE;

    protected ReadFrom readFrom;

    protected int batchSize = DEFAULT_BATCH_SIZE;

    protected String keyPattern;

    protected String keyType;

    protected MeterRegistry meterRegistry = Metrics.globalRegistry;

    protected AbstractRedisClient client;

    private OperationExecutor<K, V, KeyEvent<K>, KeyValue<K>> operationExecutor;

    private Predicate<K> keyFilter = Predicates.isTrue();

    protected RedisItemReader(RedisCodec<K, V> codec, RedisOperation<K, V, KeyEvent<K>, KeyValue<K>> operation) {
        setName(ClassUtils.getShortName(getClass()));
        this.codec = codec;
        this.operation = operation;
    }

    public List<KeyValue<K>> read(Iterable<? extends KeyEvent<K>> keys) throws Exception {
        return operationExecutor.execute(keys);
    }

    @Override
    protected synchronized void doOpen() throws Exception {
        if (operationExecutor == null) {
            operationExecutor = operationExecutor();
            operationExecutor.afterPropertiesSet();
        }
    }

    @Override
    protected void doClose() throws Exception {
        if (operationExecutor != null) {
            operationExecutor.close();
            operationExecutor = null;
        }
    }

    private OperationExecutor<K, V, KeyEvent<K>, KeyValue<K>> operationExecutor() {
        Assert.notNull(client, getName() + ": Redis client not set");
        OperationExecutor<K, V, KeyEvent<K>, KeyValue<K>> executor = new OperationExecutor<>(codec, operation);
        executor.setClient(client);
        executor.setPoolSize(poolSize);
        executor.setReadFrom(readFrom);
        return executor;
    }

    protected boolean acceptKeyType(String type) {
        return keyType == null || keyType.equalsIgnoreCase(type);
    }

    protected boolean acceptKey(K key) {
        return keyFilter.test(key);
    }

    public static RedisScanItemReader<byte[], byte[]> scanDump() {
        return new RedisScanItemReader<>(ByteArrayCodec.INSTANCE, KeyValueRead.dump(ByteArrayCodec.INSTANCE));
    }

    public static RedisScanItemReader<String, String> scanStruct() {
        return scanStruct(StringCodec.UTF8);
    }

    public static <K, V> RedisScanItemReader<K, V> scanStruct(RedisCodec<K, V> codec) {
        return new RedisScanItemReader<>(codec, KeyValueRead.struct(codec));
    }

    public static RedisScanItemReader<String, String> scanNone() {
        return scanNone(StringCodec.UTF8);
    }

    public static <K, V> RedisScanItemReader<K, V> scanNone(RedisCodec<K, V> codec) {
        return new RedisScanItemReader<>(codec, KeyValueRead.type(codec));
    }

    public static RedisLiveItemReader<byte[], byte[]> liveDump() {
        return new RedisLiveItemReader<>(ByteArrayCodec.INSTANCE, KeyValueRead.dump(ByteArrayCodec.INSTANCE));
    }

    public static RedisLiveItemReader<String, String> liveStruct() {
        return liveStruct(StringCodec.UTF8);
    }

    public static <K, V> RedisLiveItemReader<K, V> liveStruct(RedisCodec<K, V> codec) {
        return new RedisLiveItemReader<>(codec, KeyValueRead.struct(codec));
    }

    public static RedisLiveItemReader<String, String> liveNone() {
        return liveNone(StringCodec.UTF8);
    }

    public static <K, V> RedisLiveItemReader<K, V> liveNone(RedisCodec<K, V> codec) {
        return new RedisLiveItemReader<>(codec, KeyValueRead.type(codec));
    }

    public RedisOperation<K, V, KeyEvent<K>, KeyValue<K>> getOperation() {
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

    public void setMemoryUsage(MemoryUsage usage) {
        if (operation instanceof KeyValueRead) {
            ((KeyValueRead<K, V>) operation).memoryUsage(usage);
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
