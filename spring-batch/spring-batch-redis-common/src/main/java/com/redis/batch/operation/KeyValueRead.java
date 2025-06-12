package com.redis.batch.operation;

import com.redis.batch.BatchUtils;
import com.redis.batch.InitializingOperation;
import com.redis.batch.KeyType;
import com.redis.batch.KeyValue;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.internal.LettuceAssert;

import java.time.Instant;
import java.util.*;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeyValueRead<K, V> implements InitializingOperation<K, V, K, KeyValue<K>> {

    protected enum ValueType {
        DUMP, STRUCT, NONE;
    }

    private static final String SCRIPT_FILENAME = "keyvalue.lua";

    private Evalsha<K, V, KeyValue<K>> evalsha;

    private final Function<V, String> toStringValueFunction;

    private final Function<String, V> stringValueFunction;

    private final ValueType mode;

    /**
     * Max memory usage of a key in bytes. Use -1 for no limit.
     */
    private long limit;

    /**
     * Number of sampled nested values
     */
    private int samples;

    private KeyValueRead(RedisCodec<K, V> codec, ValueType mode) {
        this.mode = mode;
        this.toStringValueFunction = BatchUtils.toStringValueFunction(codec);
        this.stringValueFunction = BatchUtils.stringValueFunction(codec);
    }

    public static KeyValueRead<byte[], byte[]> dump() {
        return dump(ByteArrayCodec.INSTANCE);
    }

    public static <K, V> KeyValueRead<K, V> dump(RedisCodec<K, V> codec) {
        return new KeyValueRead<>(codec, ValueType.DUMP);
    }

    public static KeyValueRead<String, String> struct() {
        return struct(StringCodec.UTF8);
    }

    public static <K, V> KeyValueRead<K, V> struct(RedisCodec<K, V> codec) {
        return new KeyValueRead<>(codec, ValueType.STRUCT);
    }

    public static KeyValueRead<String, String> type() {
        return type(StringCodec.UTF8);
    }

    public static <K, V> KeyValueRead<K, V> type(RedisCodec<K, V> codec) {
        return new KeyValueRead<>(codec, ValueType.NONE);
    }

    @SuppressWarnings({ "deprecation", "unchecked" })
    @Override
    public void initialize(RedisAsyncCommands<K, V> commands) throws Exception {
        evalsha = new Evalsha<>(this::keyValue, value(mode), value(limit), value(samples));
        String lua = BatchUtils.readFile(SCRIPT_FILENAME);
        String digest = commands.scriptLoad(lua)
                .get(commands.getStatefulConnection().getTimeout().toNanos(), TimeUnit.NANOSECONDS);
        evalsha.setDigest(digest);
    }

    private V value(Object object) {
        return stringValueFunction.apply(String.valueOf(object));
    }

    @Override
    public List<RedisFuture<KeyValue<K>>> execute(RedisAsyncCommands<K, V> commands, List<? extends K> items) {
        return evalsha.execute(commands, items);
    }

    private KeyValue<K> keyValue(K key, List<Object> list) {
        KeyValue<K> keyValue = new KeyValue<>();
        keyValue.setKey(key);
        Iterator<Object> iterator = list.iterator();
        long ttl = (Long) iterator.next();
        if (ttl > 0) {
            keyValue.setTtl(Instant.now().plusMillis(ttl));
        }
        if (iterator.hasNext()) {
            keyValue.setType(toString(iterator.next()));
        }
        if (iterator.hasNext()) {
            keyValue.setMemoryUsage((Long) iterator.next());
        }
        if (iterator.hasNext()) {
            Object value = iterator.next();
            if (mode == ValueType.STRUCT) {
                value = structValue(keyValue, value);
            }
            keyValue.setValue(value);
        }
        return keyValue;
    }

    @SuppressWarnings("unchecked")
    protected String toString(Object value) {
        return toStringValueFunction.apply((V) value);
    }

    public KeyValueRead<K, V> limit(long limit) {
        this.limit = limit;
        return this;
    }

    public KeyValueRead<K, V> memoryUsageSamples(int samples) {
        this.samples = samples;
        return this;
    }

    @SuppressWarnings("unchecked")
    private Object structValue(KeyValue<K> item, Object value) {
        if (value == null) {
            return value;
        }
        KeyType type = item.type();
        if (type == null) {
            return value;
        }
        switch (type) {
            case HASH:
                return map((List<Object>) value);
            case SET:
                return new HashSet<>((Collection<V>) value);
            case STREAM:
                return streamMessages(item.getKey(), (Collection<List<Object>>) value);
            case TIMESERIES:
                return timeseries((List<List<Object>>) value);
            case ZSET:
                return zset(value);
            default:
                return value;
        }
    }

    private List<Sample> timeseries(List<List<Object>> value) {
        return value.stream().map(this::sample).collect(Collectors.toList());
    }

    private List<StreamMessage<K, V>> streamMessages(K key, Collection<List<Object>> value) {
        return value.stream().map(v -> message(key, v)).collect(Collectors.toList());
    }

    private Sample sample(List<Object> sample) {
        LettuceAssert.isTrue(sample.size() == 2, "Invalid list size: " + sample.size());
        Long timestamp = (Long) sample.get(0);
        return Sample.of(timestamp, toDouble(sample.get(1)));
    }

    private double toDouble(Object value) {
        return Double.parseDouble(toString(value));
    }

    @SuppressWarnings("unchecked")
    private Map<K, V> map(List<Object> list) {
        LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < list.size(); i += 2) {
            map.put((K) list.get(i), (V) list.get(i + 1));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private java.util.Set<ScoredValue<V>> zset(Object value) {
        List<Object> list = (List<Object>) value;
        LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
        Set<ScoredValue<V>> values = new HashSet<>();
        for (int i = 0; i < list.size(); i += 2) {
            double score = toDouble(list.get(i + 1));
            values.add(ScoredValue.just(score, (V) list.get(i)));
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    private StreamMessage<K, V> message(K key, List<Object> message) {
        LettuceAssert.isTrue(message.size() == 2, "Invalid list size: " + message.size());
        String id = toString(message.get(0));
        Map<K, V> body = map((List<Object>) message.get(1));
        return new StreamMessage<>(key, id, body);
    }

}
