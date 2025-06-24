package com.redis.batch.operation;

import com.redis.batch.*;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeyValueReadOperation<K, V> implements InitializingOperation<K, V, KeyEvent<K>, KeyValueEvent<K>> {

    protected enum Mode {
        STRUCT, DUMP, NONE
    }

    private static final String SCRIPT_FILENAME = "key-value-read.lua";

    private final Function<V, String> toStringValueFunction;

    private final Function<String, V> stringValueFunction;

    private final Mode mode;

    /**
     * Max memory usage of a key in bytes. Use -1 for no limit.
     */
    private long limit;

    /**
     * Number of sampled nested values
     */
    private int samples;

    private String digest;

    private V[] args;

    private KeyValueReadOperation(RedisCodec<K, V> codec) {
        this(codec, Mode.NONE);
    }

    protected KeyValueReadOperation(RedisCodec<K, V> codec, Mode mode) {
        this.toStringValueFunction = BatchUtils.toStringValueFunction(codec);
        this.stringValueFunction = BatchUtils.stringValueFunction(codec);
        this.mode = mode;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void initialize(RedisAsyncCommands<K, V> commands) throws Exception {
        String lua = BatchUtils.readFile(SCRIPT_FILENAME);
        this.digest = commands.scriptLoad(lua).get(timeout(commands).toNanos(), TimeUnit.NANOSECONDS);
        this.args = (V[]) new Object[] { stringValue(mode.name().toLowerCase()), stringValue(limit), stringValue(samples) };
    }

    private V stringValue(Object object) {
        return stringValueFunction.apply(String.valueOf(object));
    }

    @Override
    public List<RedisFuture<KeyValueEvent<K>>> execute(RedisAsyncCommands<K, V> commands, List<? extends KeyEvent<K>> items) {
        return items.stream().map(e -> execute(commands, e)).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private RedisFuture<KeyValueEvent<K>> execute(RedisAsyncCommands<K, V> commands, KeyEvent<K> keyEvent) {
        K[] keys = (K[]) new Object[] { keyEvent.getKey() };
        RedisFuture<List<Object>> evalFuture = commands.evalsha(digest, ScriptOutputType.MULTI, keys, args);
        return new MappingRedisFuture<>(evalFuture, l -> keyValueEvent(keyEvent, l));
    }

    private KeyValueEvent<K> keyValueEvent(KeyEvent<K> keyEvent, List<Object> list) {
        ReadResult<V> result = result(list);
        KeyValueEvent<K> keyValue = new KeyValueEvent<>();
        keyValue.setEvent(keyEvent.getEvent());
        keyValue.setKey(keyEvent.getKey());
        keyValue.setOperation(keyEvent.getOperation());
        keyValue.setTimestamp(keyEvent.getTimestamp());
        keyValue.setType(toString(result.getType()));
        if (result.getTtl() != null && result.getTtl() >= 0) {
            keyValue.setTtl(Instant.now().plusMillis(result.getTtl()));
        }
        if (result.getValue() != null) {
            keyValue.setValue(value(result));
        }
        return keyValue;
    }

    protected Object value(ReadResult<V> result) {
        return null;
    }

    @SuppressWarnings("unchecked")
    protected ReadResult<V> result(List<Object> list) {
        ReadResult<V> result = new ReadResult<>();
        Iterator<Object> iterator = list.iterator();
        if (iterator.hasNext()) {
            result.ttl = (Long) iterator.next();
        }
        if (iterator.hasNext()) {
            result.type = (V) iterator.next();
        }
        if (iterator.hasNext()) {
            result.value = iterator.next();
        }
        return result;
    }

    protected static class ReadResult<V> {

        private Long ttl;

        private V type;

        private Object value;

        public Long getTtl() {
            return ttl;
        }

        public V getType() {
            return type;
        }

        public Object getValue() {
            return value;
        }

    }

    protected String toString(V value) {
        return toStringValueFunction.apply(value);
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public void setSamples(int samples) {
        this.samples = samples;
    }

    public static KeyDumpReadOperation dump() {
        return new KeyDumpReadOperation();
    }

    public static <K, V> KeyStructReadOperation<K, V> struct(RedisCodec<K, V> codec) {
        return new KeyStructReadOperation<>(codec);
    }

    public static <K, V> KeyValueReadOperation<K, V> none(RedisCodec<K, V> codec) {
        return new KeyValueReadOperation<>(codec);
    }

}
