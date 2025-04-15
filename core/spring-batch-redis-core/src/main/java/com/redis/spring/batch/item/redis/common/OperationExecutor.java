package com.redis.spring.batch.item.redis.common;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.core.observability.BatchMetrics;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.BatchRedisMetrics;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer.Sample;

public class OperationExecutor<K, V, I, O> implements InitializingBean, AutoCloseable {

    public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

    public static final String TIMER_NAME = "operation";

    public static final String TIMER_DESCRIPTION = "Operation execution duration";

    private final RedisOperation<K, V, I, O> operation;

    private final RedisCodec<K, V> codec;

    private AbstractRedisClient client;

    private ReadFrom readFrom;

    private int poolSize = DEFAULT_POOL_SIZE;

    private MeterRegistry meterRegistry = Metrics.globalRegistry;

    private GenericObjectPool<StatefulRedisModulesConnection<K, V>> pool;

    public OperationExecutor(RedisCodec<K, V> codec, RedisOperation<K, V, I, O> operation) {
        this.codec = codec;
        this.operation = operation;
    }

    public void setClient(AbstractRedisClient client) {
        this.client = client;
    }

    public void setMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(client, "Redis client not set");
        GenericObjectPoolConfig<StatefulRedisModulesConnection<K, V>> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(poolSize);
        Supplier<StatefulRedisModulesConnection<K, V>> supplier = BatchUtils.supplier(client, codec, readFrom);
        pool = ConnectionPoolSupport.createGenericObjectPool(supplier, config);
        initializeOperation();
    }

    private void initializeOperation() throws Exception {
        if (operation instanceof InitializingOperation) {
            try (StatefulRedisModulesConnection<K, V> connection = pool.borrowObject()) {
                ((InitializingOperation<K, V, I, O>) operation).initialize(connection.async());
            }
        }
    }

    @Override
    public synchronized void close() {
        if (pool != null) {
            pool.close();
            pool = null;
        }
    }

    public List<O> execute(Iterable<? extends I> items) throws Exception {
        Sample sample = BatchMetrics.createTimerSample(meterRegistry);
        String status = BatchMetrics.STATUS_SUCCESS;
        try (StatefulRedisModulesConnection<K, V> connection = pool.borrowObject()) {
            connection.setAutoFlushCommands(false);
            try {
                return execute(connection, items);
            } catch (RedisNoScriptException e) {
                // Potential fail-over of Redis shard(s). Need to reload the LUA script.
                initializeOperation();
                return execute(connection, items);
            } finally {
                connection.setAutoFlushCommands(true);
            }
        } catch (Exception e) {
            status = BatchMetrics.STATUS_FAILURE;
            throw e;
        } finally {
            sample.stop(BatchRedisMetrics.createTimer(meterRegistry, TIMER_NAME, TIMER_DESCRIPTION, Tag.of("status", status)));
        }
    }

    private List<O> execute(StatefulRedisModulesConnection<K, V> connection, Iterable<? extends I> items)
            throws TimeoutException, InterruptedException, ExecutionException {
        List<RedisFuture<O>> futures = operation.execute(connection.async(), items);
        connection.flushCommands();
        return BatchUtils.getAll(connection.getTimeout(), futures);
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public ReadFrom getReadFrom() {
        return readFrom;
    }

    public int getPoolSize() {
        return poolSize;
    }

}
