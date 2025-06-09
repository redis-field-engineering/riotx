package com.redis.spring.batch.item.redis;

import com.redis.batch.KeyValue;
import com.redis.batch.OperationExecutor;
import com.redis.batch.RedisOperation;
import com.redis.batch.Wait;
import com.redis.spring.batch.item.redis.common.RedisSupportCheck;
import com.redis.batch.operation.KeyValueRestore;
import com.redis.batch.operation.KeyValueWrite;
import com.redis.batch.operation.KeyValueWrite.WriteMode;
import com.redis.batch.operation.MultiExec;
import com.redis.batch.operation.ReplicaWait;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.ToString;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

@ToString
public class RedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> {

    public static final int DEFAULT_POOL_SIZE = OperationExecutor.DEFAULT_POOL_SIZE;

    public static final Wait DEFAULT_WAIT = new Wait();

    private final RedisCodec<K, V> codec;

    private final RedisOperation<K, V, T, Object> operation;

    private AbstractRedisClient client;

    private boolean multiExec;

    private Wait wait = DEFAULT_WAIT;

    private int poolSize = DEFAULT_POOL_SIZE;

    private OperationExecutor<K, V, T, Object> operationExecutor;

    private RedisSupportCheck redisSupportCheck = new RedisSupportCheck();

    public RedisItemWriter(RedisCodec<K, V> codec, RedisOperation<K, V, T, Object> operation) {
        setName(ClassUtils.getShortName(getClass()));
        this.codec = codec;
        this.operation = operation;
    }

    @SuppressWarnings("unchecked")
    public void setMode(WriteMode mode) {
        if (operation instanceof KeyValueWrite) {
            ((KeyValueWrite<K, V>) operation).setMode(mode);
        }
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        Assert.notNull(client, "Redis client not set"); //$NON-NLS-1$
        if (operationExecutor == null) {
            redisSupportCheck.accept(client);
            operationExecutor = new OperationExecutor<>(codec, operation());
            operationExecutor.setClient(client);
            operationExecutor.setPoolSize(poolSize);
            try {
                operationExecutor.afterPropertiesSet();
            } catch (Exception e) {
                throw new ItemStreamException("Could not initialize operation executor", e);
            }
        }
    }

    @Override
    public synchronized void close() {
        if (operationExecutor != null) {
            operationExecutor.close();
            operationExecutor = null;
        }
    }

    public boolean isOpen() {
        return operationExecutor != null;
    }

    @Override
    public void write(Chunk<? extends T> items) throws Exception {
        operationExecutor.execute(items.getItems());
    }

    private RedisOperation<K, V, T, Object> operation() {
        return multiExec(waitReplicas(operation));
    }

    private RedisOperation<K, V, T, Object> waitReplicas(RedisOperation<K, V, T, Object> operation) {
        if (wait.getReplicas() > 0) {
            return new ReplicaWait<>(operation, wait.getReplicas(), wait.getTimeout());
        }
        return operation;
    }

    private RedisOperation<K, V, T, Object> multiExec(RedisOperation<K, V, T, Object> operation) {
        if (multiExec) {
            return new MultiExec<>(operation);
        }
        return operation;
    }

    public static <T> RedisItemWriter<String, String, T> operation(RedisOperation<String, String, T, Object> operation) {
        return new RedisItemWriter<>(StringCodec.UTF8, operation);
    }

    public static RedisItemWriter<byte[], byte[], KeyValue<byte[]>> dump() {
        return new RedisItemWriter<>(ByteArrayCodec.INSTANCE, new KeyValueRestore<>());
    }

    public static RedisItemWriter<String, String, KeyValue<String>> struct() {
        return struct(StringCodec.UTF8);
    }

    public static <K, V> RedisItemWriter<K, V, KeyValue<K>> struct(RedisCodec<K, V> codec) {
        return new RedisItemWriter<>(codec, new KeyValueWrite<>());
    }

    public RedisSupportCheck getRedisSupportCheck() {
        return redisSupportCheck;
    }

    public void setRedisSupportCheck(RedisSupportCheck check) {
        this.redisSupportCheck = check;
    }

    public AbstractRedisClient getClient() {
        return client;
    }

    public void setClient(AbstractRedisClient client) {
        this.client = client;
    }

    public Wait getWait() {
        return wait;
    }

    public void setWait(Wait wait) {
        this.wait = wait;
    }

    public boolean getMultiExec() {
        return multiExec;
    }

    public void setMultiExec(boolean multiExec) {
        this.multiExec = multiExec;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

}
