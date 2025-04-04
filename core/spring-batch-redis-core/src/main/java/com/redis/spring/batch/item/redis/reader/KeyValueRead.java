package com.redis.spring.batch.item.redis.reader;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.InitializingOperation;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class KeyValueRead<K, V, T> implements InitializingOperation<K, V, KeyEvent<K>, KeyValue<K>> {

    protected enum ValueType {
        DUMP, STRUCT, NONE
    }

    public static final long NO_MEM_USAGE = -1;

    public static final long DEFAULT_MEM_USAGE_LIMIT = NO_MEM_USAGE;

    public static final int DEFAULT_MEM_USAGE_SAMPLES = 5;

    public static final String ATTR_MEMORY_USAGE = "memory-usage";

    private static final String SCRIPT_FILENAME = "keyvalue.lua";

    private final RedisCodec<K, V> codec;

    private final Evalsha<K, V, KeyEvent<K>, KeyValue<K>> evalsha;

    private final Function<V, String> toStringValueFunction;

    private final ValueType mode;

    private AbstractRedisClient client;

    private long memUsageLimit = DEFAULT_MEM_USAGE_LIMIT;

    private int memUsageSamples = DEFAULT_MEM_USAGE_SAMPLES;

    public KeyValueRead(ValueType mode, RedisCodec<K, V> codec) {
        this.mode = mode;
        this.codec = codec;
        this.evalsha = new Evalsha<>(codec, KeyEvent::getKey, this::convert);
        this.toStringValueFunction = BatchUtils.toStringValueFunction(codec);
    }

    public void setClient(AbstractRedisClient client) {
        this.client = client;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(client, "Redis client not set");
        evalsha.setArgs(mode, memUsageLimit, memUsageSamples);
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
            keyValue.setValue(value(keyValue, iterator.next()));
        }
        return keyValue;
    }

    protected Object value(KeyValue<K> keyValue, Object value) {
        return value;
    }

    @SuppressWarnings("unchecked")
    protected String toString(Object value) {
        return toStringValueFunction.apply((V) value);
    }

    /**
     * 
     * @return max memory usage in bytes
     */
    public long getMemUsageLimit() {
        return memUsageLimit;
    }

    /**
     * 
     * @param limit max memory usage in bytes
     */
    public void setMemUsageLimit(long limit) {
        this.memUsageLimit = limit;
    }

    public int getMemUsageSamples() {
        return memUsageSamples;
    }

    public void setMemUsageSamples(int samples) {
        this.memUsageSamples = samples;
    }

    public static <K, V> KeyValueRead<K, V, byte[]> dump(RedisCodec<K, V> codec) {
        return new KeyValueRead<>(ValueType.DUMP, codec);
    }

    public static <K, V> KeyValueRead<K, V, Object> type(RedisCodec<K, V> codec) {
        return new KeyValueRead<>(ValueType.NONE, codec);
    }

    public static <K, V> KeyValueRead<K, V, Object> struct(RedisCodec<K, V> codec) {
        return new KeyValueStructRead<>(codec);
    }

}
