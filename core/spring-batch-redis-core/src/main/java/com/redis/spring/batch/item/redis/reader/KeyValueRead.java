package com.redis.spring.batch.item.redis.reader;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.InitializingOperation;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.internal.LettuceAssert;

public class KeyValueRead<K, V> implements InitializingOperation<K, V, KeyEvent<K>, KeyValue<K>> {

    protected enum ValueType {
        DUMP, STRUCT, NONE
    }

    public static final String ATTR_MEMORY_USAGE = "memory-usage";

    private static final String SCRIPT_FILENAME = "keyvalue.lua";

    private final RedisCodec<K, V> codec;

    private final Evalsha<K, V, KeyEvent<K>, KeyValue<K>> evalsha;

    private final Function<V, String> toStringValueFunction;

    private final ValueType mode;

    private AbstractRedisClient client;

    private MemoryUsage memoryUsage = MemoryUsage.of(MemoryUsage.DISABLED);

    public KeyValueRead(RedisCodec<K, V> codec, ValueType mode) {
        this.codec = codec;
        this.mode = mode;
        this.evalsha = new Evalsha<>(codec, KeyEvent::getKey, this::convert);
        this.toStringValueFunction = BatchUtils.toStringValueFunction(codec);
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

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(client, "Redis client not set");
        evalsha.setArgs(mode, memoryUsage.getLimit().toBytes(), memoryUsage.getSamples());
        String lua = BatchUtils.readFile(SCRIPT_FILENAME);
        try (StatefulRedisModulesConnection<K, V> connection = BatchUtils.connection(client, codec)) {
            String digest = connection.sync().scriptLoad(lua);
            evalsha.setDigest(digest);
        }
    }

    @Override
    public List<RedisFuture<KeyValue<K>>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends KeyEvent<K>> items) {
        return evalsha.execute(commands, items);
    }

    private KeyValue<K> convert(KeyEvent<K> keyEvent, List<Object> list) {
        KeyValue<K> keyValue = new KeyValue<>();
        keyValue.setEvent(keyEvent.getEvent());
        keyValue.setKey(keyEvent.getKey());
        keyValue.setTimestamp(keyEvent.getTimestamp());
        keyValue.setType(keyEvent.getType());
        Iterator<Object> iterator = list.iterator();
        keyValue.setTtl((Long) iterator.next());
        if (iterator.hasNext()) {
            keyValue.setMemoryUsage((Long) iterator.next());
        }
        if (iterator.hasNext()) {
            keyValue.setType(toString(iterator.next()));
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

    @Override
    public void setClient(AbstractRedisClient client) {
        this.client = client;
    }

    public KeyValueRead<K, V> memoryUsage(MemoryUsage usage) {
        this.memoryUsage = usage;
        return this;
    }

    @SuppressWarnings("unchecked")
    private Object structValue(KeyValue<K> item, Object value) {
        if (value == null || item.getType() == null) {
            return value;
        }
        switch (item.getType()) {
            case KeyValue.TYPE_HASH:
                return map((List<Object>) value);
            case KeyValue.TYPE_SET:
                return new HashSet<>((Collection<V>) value);
            case KeyValue.TYPE_STREAM:
                return streamMessages(item.getKey(), (Collection<List<Object>>) value);
            case KeyValue.TYPE_TIMESERIES:
                return timeseries((List<List<Object>>) value);
            case KeyValue.TYPE_ZSET:
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
    private Set<ScoredValue<V>> zset(Object value) {
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
