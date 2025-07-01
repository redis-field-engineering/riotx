package com.redis.batch.operation;

import com.redis.batch.*;
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

public class KeyStructWrite<K, V> implements RedisBatchOperation<K, V, KeyStructEvent<K, V>, Object> {

    public enum WriteMode {
        MERGE, OVERWRITE
    }

    public static final WriteMode DEFAULT_MODE = WriteMode.OVERWRITE;

    private final Del<K, V, KeyStructEvent<K, V>> delete = delete();

    private final ExpireAt<K, V, KeyStructEvent<K, V>> expire = expire();

    private final ClassifierOperation<K, V, KeyStructEvent<K, V>, KeyType> write = writeOperation();

    private WriteMode mode = DEFAULT_MODE;

    @Override
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends KeyStructEvent<K, V>> items) {
        List<Future<Object>> futures = new ArrayList<>(delete.execute(commands, toDelete(items)));
        futures.addAll(write.execute(commands, toWrite(items)));
        futures.addAll(expire.execute(commands, toExpire(items)));
        return futures;
    }

    private List<KeyStructEvent<K, V>> toWrite(Iterable<? extends KeyStructEvent<K, V>> items) {
        return stream(items, t -> t.getOperation() != KeyOperation.DELETE).collect(Collectors.toList());
    }

    private ClassifierOperation<K, V, KeyStructEvent<K, V>, KeyType> writeOperation() {
        ClassifierOperation<K, V, KeyStructEvent<K, V>, KeyType> operation = new ClassifierOperation<>(
                KeyStructEvent::getType);
        operation.setOperation(KeyType.hash, new Hset<>(KeyStructEvent::getKey, KeyStructEvent::asHash));
        operation.setOperation(KeyType.json, new JsonSet<>(KeyStructEvent::getKey, KeyStructEvent::asJson));
        operation.setOperation(KeyType.string, new Set<>(KeyStructEvent::getKey, KeyStructEvent::asString));
        operation.setOperation(KeyType.list, new Rpush<>(KeyStructEvent::getKey, KeyStructEvent::asList));
        operation.setOperation(KeyType.set, new Sadd<>(KeyStructEvent::getKey, KeyStructEvent::asSet));
        operation.setOperation(KeyType.stream, new Xadd<>(KeyStructEvent::getKey, KeyStructEvent::asStream));
        TsAdd<K, V, KeyStructEvent<K, V>> tsAdd = new TsAdd<>(KeyStructEvent::getKey, KeyStructEvent::asTimeseries);
        tsAdd.setOptions(AddOptions.<K, V> builder().policy(DuplicatePolicy.LAST).build());
        operation.setOperation(KeyType.timeseries, tsAdd);
        operation.setOperation(KeyType.zset, new Zadd<>(KeyStructEvent::getKey, KeyStructEvent::asZSet));
        return operation;
    }

    private Del<K, V, KeyStructEvent<K, V>> delete() {
        return new Del<>(KeyStructEvent::getKey);
    }

    private ExpireAt<K, V, KeyStructEvent<K, V>> expire() {
        ExpireAt<K, V, KeyStructEvent<K, V>> operation = new ExpireAt<>(KeyStructEvent::getKey);
        operation.setTimestampFunction(KeyTtlTypeEvent::getTtl);
        return operation;
    }

    private List<KeyStructEvent<K, V>> toExpire(Iterable<? extends KeyStructEvent<K, V>> items) {
        return stream(items, t -> t.getTtl() != null).collect(Collectors.toList());
    }

    private List<KeyStructEvent<K, V>> toDelete(Iterable<? extends KeyStructEvent<K, V>> items) {
        return stream(items, this::shouldDelete).collect(Collectors.toList());
    }

    private Stream<? extends KeyStructEvent<K, V>> stream(Iterable<? extends KeyStructEvent<K, V>> items,
            Predicate<KeyStructEvent<K, V>> filter) {
        return StreamSupport.stream(items.spliterator(), false).filter(filter);
    }

    private boolean shouldDelete(KeyStructEvent<K, V> item) {
        return mode == WriteMode.OVERWRITE || item.getOperation() == KeyOperation.DELETE;
    }

    public void setMode(WriteMode mode) {
        this.mode = mode;
    }

    public Del<K, V, KeyStructEvent<K, V>> getDelete() {
        return delete;
    }

    public ExpireAt<K, V, KeyStructEvent<K, V>> getExpire() {
        return expire;
    }

    public ClassifierOperation<K, V, KeyStructEvent<K, V>, KeyType> getWrite() {
        return write;
    }

    public WriteMode getMode() {
        return mode;
    }

}
