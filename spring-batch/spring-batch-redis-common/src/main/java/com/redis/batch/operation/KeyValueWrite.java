package com.redis.batch.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.redis.batch.KeyType;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.batch.KeyValue;
import com.redis.batch.RedisOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class KeyValueWrite<K, V> implements RedisOperation<K, V, KeyValue<K>, Object> {

    public enum WriteMode {
        MERGE, OVERWRITE
    }

    public static final WriteMode DEFAULT_MODE = WriteMode.OVERWRITE;

    private final Del<K, V, KeyValue<K>> delete = delete();

    private final ExpireAt<K, V, KeyValue<K>> expire = expire();

    private final ClassifierOperation<K, V, KeyValue<K>, KeyType> write = writeOperation();

    private WriteMode mode = DEFAULT_MODE;

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends KeyValue<K>> items) {
        List<RedisFuture<Object>> futures = new ArrayList<>();
        List<KeyValue<K>> toDelete = toDelete(items);
        futures.addAll(delete.execute(commands, toDelete));
        List<KeyValue<K>> toWrite = toWrite(items);
        futures.addAll(write.execute(commands, toWrite));
        List<KeyValue<K>> toExpire = toExpire(items);
        futures.addAll(expire.execute(commands, toExpire));
        return futures;
    }

    private List<KeyValue<K>> toWrite(Iterable<? extends KeyValue<K>> items) {
        return stream(items).filter(t -> t.getValue() != null).collect(Collectors.toList());
    }

    private ClassifierOperation<K, V, KeyValue<K>, KeyType> writeOperation() {
        ClassifierOperation<K, V, KeyValue<K>, KeyType> operation = new ClassifierOperation<>(KeyValue::type);
        operation.setOperation(KeyType.HASH, new Hset<>(KeyValue::getKey, KeyValueWrite::value));
        operation.setOperation(KeyType.JSON, new JsonSet<>(KeyValue::getKey, KeyValueWrite::value));
        operation.setOperation(KeyType.STRING, new Set<>(KeyValue::getKey, KeyValueWrite::value));
        operation.setOperation(KeyType.LIST, new Rpush<>(KeyValue::getKey, KeyValueWrite::value));
        operation.setOperation(KeyType.SET, new Sadd<>(KeyValue::getKey, KeyValueWrite::value));
        operation.setOperation(KeyType.STREAM, new Xadd<>(KeyValue::getKey, KeyValueWrite::value));
        TsAdd<K, V, KeyValue<K>> tsAdd = new TsAdd<>(KeyValue::getKey, KeyValueWrite::value);
        tsAdd.setOptions(AddOptions.<K, V> builder().policy(DuplicatePolicy.LAST).build());
        operation.setOperation(KeyType.TIMESERIES, tsAdd);
        operation.setOperation(KeyType.ZSET, new Zadd<>(KeyValue::getKey, KeyValueWrite::value));
        return operation;
    }

    private Del<K, V, KeyValue<K>> delete() {
        return new Del<>(KeyValue::getKey);
    }

    private ExpireAt<K, V, KeyValue<K>> expire() {
        ExpireAt<K, V, KeyValue<K>> operation = new ExpireAt<>(KeyValue::getKey);
        operation.setTimestampFunction(KeyValue::getTtl);
        return operation;
    }

    private List<KeyValue<K>> toExpire(Iterable<? extends KeyValue<K>> items) {
        return stream(items).filter(t -> t.getTtl() != null).collect(Collectors.toList());
    }

    private List<KeyValue<K>> toDelete(Iterable<? extends KeyValue<K>> items) {
        return stream(items).filter(this::shouldDelete).collect(Collectors.toList());
    }

    private Stream<? extends KeyValue<K>> stream(Iterable<? extends KeyValue<K>> items) {
        return StreamSupport.stream(items.spliterator(), false);
    }

    private boolean shouldDelete(KeyValue<K> item) {
        return mode == WriteMode.OVERWRITE || item.type() == KeyType.NONE;
    }

    @SuppressWarnings("unchecked")
    private static <K, O> O value(KeyValue<K> struct) {
        return (O) struct.getValue();
    }

    public void setMode(WriteMode mode) {
        this.mode = mode;
    }

    public Del<K, V, KeyValue<K>> getDelete() {
        return delete;
    }

    public ExpireAt<K, V, KeyValue<K>> getExpire() {
        return expire;
    }

    public ClassifierOperation<K, V, KeyValue<K>, KeyType> getWrite() {
        return write;
    }

    public WriteMode getMode() {
        return mode;
    }

}
