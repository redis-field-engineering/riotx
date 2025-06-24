package com.redis.batch.operation;

import com.redis.batch.KeyOperation;
import com.redis.batch.KeyType;
import com.redis.batch.KeyValueEvent;
import com.redis.batch.RedisBatchOperation;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KeyValueWrite<K, V> implements RedisBatchOperation<K, V, KeyValueEvent<K>, Object> {

    public enum WriteMode {
        MERGE, OVERWRITE
    }

    public static final WriteMode DEFAULT_MODE = WriteMode.OVERWRITE;

    private final Del<K, V, KeyValueEvent<K>> delete = delete();

    private final ExpireAt<K, V, KeyValueEvent<K>> expire = expire();

    private final ClassifierOperation<K, V, KeyValueEvent<K>, KeyType> write = writeOperation();

    private WriteMode mode = DEFAULT_MODE;

    @Override
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends KeyValueEvent<K>> items) {
        List<Future<Object>> futures = new ArrayList<>(delete.execute(commands, toDelete(items)));
        futures.addAll(write.execute(commands, toWrite(items)));
        futures.addAll(expire.execute(commands, toExpire(items)));
        return futures;
    }

    private List<KeyValueEvent<K>> toWrite(Iterable<? extends KeyValueEvent<K>> items) {
        return stream(items, t -> t.getOperation() != KeyOperation.DELETE).collect(Collectors.toList());
    }

    private ClassifierOperation<K, V, KeyValueEvent<K>, KeyType> writeOperation() {
        ClassifierOperation<K, V, KeyValueEvent<K>, KeyType> operation = new ClassifierOperation<>(
                e -> KeyType.of(e.getType()));
        operation.setOperation(KeyType.HASH, new Hset<>(KeyValueEvent::getKey, this::value));
        operation.setOperation(KeyType.JSON, new JsonSet<>(KeyValueEvent::getKey, this::value));
        operation.setOperation(KeyType.STRING, new Set<>(KeyValueEvent::getKey, this::value));
        operation.setOperation(KeyType.LIST, new Rpush<>(KeyValueEvent::getKey, this::value));
        operation.setOperation(KeyType.SET, new Sadd<>(KeyValueEvent::getKey, this::value));
        operation.setOperation(KeyType.STREAM, new Xadd<>(KeyValueEvent::getKey, this::value));
        TsAdd<K, V, KeyValueEvent<K>> tsAdd = new TsAdd<>(KeyValueEvent::getKey, this::value);
        tsAdd.setOptions(AddOptions.<K, V> builder().policy(DuplicatePolicy.LAST).build());
        operation.setOperation(KeyType.TIMESERIES, tsAdd);
        operation.setOperation(KeyType.ZSET, new Zadd<>(KeyValueEvent::getKey, this::value));
        return operation;
    }

    private Del<K, V, KeyValueEvent<K>> delete() {
        return new Del<>(KeyValueEvent::getKey);
    }

    private ExpireAt<K, V, KeyValueEvent<K>> expire() {
        ExpireAt<K, V, KeyValueEvent<K>> operation = new ExpireAt<>(KeyValueEvent::getKey);
        operation.setTimestampFunction(KeyValueEvent::getTtl);
        return operation;
    }

    private List<KeyValueEvent<K>> toExpire(Iterable<? extends KeyValueEvent<K>> items) {
        return stream(items, t -> t.getTtl() != null).collect(Collectors.toList());
    }

    private List<KeyValueEvent<K>> toDelete(Iterable<? extends KeyValueEvent<K>> items) {
        return stream(items, this::shouldDelete).collect(Collectors.toList());
    }

    private Stream<? extends KeyValueEvent<K>> stream(Iterable<? extends KeyValueEvent<K>> items,
            Predicate<KeyValueEvent<K>> filter) {
        return StreamSupport.stream(items.spliterator(), false).filter(filter);
    }

    private boolean shouldDelete(KeyValueEvent<K> item) {
        return mode == WriteMode.OVERWRITE || item.getOperation() == KeyOperation.DELETE;
    }

    @SuppressWarnings("unchecked")
    private <O> O value(KeyValueEvent<K> struct) {
        return (O) struct.getValue();
    }

    public void setMode(WriteMode mode) {
        this.mode = mode;
    }

    public Del<K, V, KeyValueEvent<K>> getDelete() {
        return delete;
    }

    public ExpireAt<K, V, KeyValueEvent<K>> getExpire() {
        return expire;
    }

    public ClassifierOperation<K, V, KeyValueEvent<K>, KeyType> getWrite() {
        return write;
    }

    public WriteMode getMode() {
        return mode;
    }

}
