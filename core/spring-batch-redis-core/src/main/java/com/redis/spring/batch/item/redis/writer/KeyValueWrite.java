package com.redis.spring.batch.item.redis.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.RedisOperation;
import com.redis.spring.batch.item.redis.writer.impl.ClassifierOperation;
import com.redis.spring.batch.item.redis.writer.impl.Del;
import com.redis.spring.batch.item.redis.writer.impl.ExpireAt;
import com.redis.spring.batch.item.redis.writer.impl.Hset;
import com.redis.spring.batch.item.redis.writer.impl.JsonSet;
import com.redis.spring.batch.item.redis.writer.impl.Rpush;
import com.redis.spring.batch.item.redis.writer.impl.Sadd;
import com.redis.spring.batch.item.redis.writer.impl.Set;
import com.redis.spring.batch.item.redis.writer.impl.TsAdd;
import com.redis.spring.batch.item.redis.writer.impl.Xadd;
import com.redis.spring.batch.item.redis.writer.impl.Zadd;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class KeyValueWrite<K, V> implements RedisOperation<K, V, KeyValue<K>, Object> {

    public enum WriteMode {
        MERGE, OVERWRITE
    }

    public static final WriteMode DEFAULT_MODE = WriteMode.OVERWRITE;

    private final Del<K, V, KeyValue<K>> delete = delete();

    private final ExpireAt<K, V, KeyValue<K>> expire = expire();

    private final ClassifierOperation<K, V, KeyValue<K>, String> write = writeOperation();

    private WriteMode mode = DEFAULT_MODE;

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends KeyValue<K>> items) {
        List<RedisFuture<Object>> futures = new ArrayList<>();
        futures.addAll(delete.execute(commands, toDelete(items)));
        futures.addAll(write.execute(commands, toWrite(items)));
        futures.addAll(expire.execute(commands, toExpire(items)));
        return futures;
    }

    private List<? extends KeyValue<K>> toWrite(Iterable<? extends KeyValue<K>> items) {
        return stream(items).filter(KeyValue::exists).collect(Collectors.toList());
    }

    private ClassifierOperation<K, V, KeyValue<K>, String> writeOperation() {
        ClassifierOperation<K, V, KeyValue<K>, String> operation = new ClassifierOperation<>(KeyValue::getType);
        operation.setOperation(KeyValue.TYPE_HASH, new Hset<>(KeyValue::getKey, KeyValueWrite::value));
        operation.setOperation(KeyValue.TYPE_JSON, new JsonSet<>(KeyValue::getKey, KeyValueWrite::value));
        operation.setOperation(KeyValue.TYPE_STRING, new Set<>(KeyValue::getKey, KeyValueWrite::value));
        operation.setOperation(KeyValue.TYPE_LIST, new Rpush<>(KeyValue::getKey, KeyValueWrite::value));
        operation.setOperation(KeyValue.TYPE_SET, new Sadd<>(KeyValue::getKey, KeyValueWrite::value));
        operation.setOperation(KeyValue.TYPE_STREAM, new Xadd<>(KeyValue::getKey, KeyValueWrite::value));
        TsAdd<K, V, KeyValue<K>> tsAdd = new TsAdd<>(KeyValue::getKey, KeyValueWrite::value);
        tsAdd.setOptions(AddOptions.<K, V> builder().policy(DuplicatePolicy.LAST).build());
        operation.setOperation(KeyValue.TYPE_TIMESERIES, tsAdd);
        operation.setOperation(KeyValue.TYPE_ZSET, new Zadd<>(KeyValue::getKey, KeyValueWrite::value));
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
        return stream(items).filter(KeyValue::hasTtl).collect(Collectors.toList());
    }

    private List<KeyValue<K>> toDelete(Iterable<? extends KeyValue<K>> items) {
        return stream(items).filter(this::shouldDelete).collect(Collectors.toList());
    }

    private Stream<? extends KeyValue<K>> stream(Iterable<? extends KeyValue<K>> items) {
        return StreamSupport.stream(items.spliterator(), false);
    }

    private boolean shouldDelete(KeyValue<K> item) {
        return mode == WriteMode.OVERWRITE || !KeyValue.exists(item);
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

    public ClassifierOperation<K, V, KeyValue<K>, String> getWrite() {
        return write;
    }

    public WriteMode getMode() {
        return mode;
    }

    public static <K, V> KeyValueWrite<K, V> create(WriteMode mode) {
        KeyValueWrite<K, V> operation = new KeyValueWrite<>();
        operation.setMode(mode);
        return operation;
    }

}
