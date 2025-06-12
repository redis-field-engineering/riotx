package com.redis.batch;

import com.hrakaroo.glob.GlobPattern;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class BatchUtils {

    public static final String METRICS_PREFIX = "redis.batch.";

    public static final String SIZE_SUFFIX = ".size";

    public static final String CAPACITY_SUFFIX = ".capacity";

    public static final String TAG_NONE = "none";

    public static final String STATUS_SUCCESS = "SUCCESS";

    public static final String STATUS_FAILURE = "FAILURE";

    public static final Function<String, byte[]> STRING_KEY_TO_BYTES = toByteArrayKeyFunction(StringCodec.UTF8);

    public static final Function<String, byte[]> STRING_VALUE_TO_BYTES = toByteArrayValueFunction(StringCodec.UTF8);

    private BatchUtils() {
    }

    public static <T extends BlockingQueue<?>> T gaugeQueue(MeterRegistry meterRegistry, String name, T queue, Tag... tags) {
        Gauge.builder(METRICS_PREFIX + name + SIZE_SUFFIX, queue, BlockingQueue::size).tags(Arrays.asList(tags))
                .description("Gauge reflecting the size (depth) of the queue").register(meterRegistry);
        Gauge.builder(METRICS_PREFIX + name + CAPACITY_SUFFIX, queue, BlockingQueue::remainingCapacity)
                .tags(Arrays.asList(tags)).description("Gauge reflecting the remaining capacity of the queue")
                .register(meterRegistry);
        return queue;
    }

    /**
     * Create a {@link io.micrometer.core.instrument.Timer}.
     *
     * @param meterRegistry the meter registry to use
     * @param name of the timer. Will be prefixed with {@link #METRICS_PREFIX}.
     * @param description of the timer
     * @param tags of the timer
     * @return a new timer instance
     */
    public static io.micrometer.core.instrument.Timer createTimer(MeterRegistry meterRegistry, String name, String description,
            Tag... tags) {
        return Timer.builder(METRICS_PREFIX + name).description(description).tags(Arrays.asList(tags)).register(meterRegistry);
    }

    /**
     * Create a {@link Counter}.
     *
     * @param meterRegistry the meter registry to use
     * @param name of the counter. Will be prefixed with {@link #METRICS_PREFIX}.
     * @param description of the counter
     * @param tags of the counter
     * @return a new timer instance
     */
    public static Counter createCounter(MeterRegistry meterRegistry, String name, String description, Tag... tags) {
        return createCounter(meterRegistry, name, description, Arrays.asList(tags));
    }

    public static Counter createCounter(MeterRegistry meterRegistry, String name, String description, Iterable<Tag> tags) {
        return Counter.builder(METRICS_PREFIX + name).description(description).tags(tags).register(meterRegistry);
    }

    public static Gauge createGauge(MeterRegistry meterRegistry, String name, Supplier<Number> f, String description,
            Tag... tags) {
        return Gauge.builder(METRICS_PREFIX + name, f).description(description).tags(Arrays.asList(tags))
                .register(meterRegistry);
    }

    public static String tagValue(String value) {
        return value == null ? TAG_NONE : value;
    }

    public static boolean hasLength(CharSequence str) {
        return (str != null && !str.isEmpty());  // as of JDK 15
    }

    public static boolean isEmpty(Collection<?> collection) {
        return (collection == null || collection.isEmpty());
    }

    public static boolean isEmpty(Map<?, ?> map) {
        return (map == null || map.isEmpty());
    }

    public static Predicate<String> globPredicate(String pattern) {
        if (hasLength(pattern)) {
            return GlobPattern.compile(pattern)::matches;
        }
        return t -> true;
    }

    public static Tags tags(KeyValue<?> item, String status) {
        return Tags.of("event", tagValue(item.getEvent()), "status", status, "type", tagValue(item.getType()));
    }

    public static String readFile(String filename) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(BatchUtils.class.getClassLoader().getResourceAsStream(filename),
                        StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    public static <K, V> Iterator<K> scanIterator(StatefulRedisClusterConnection<K, V> connection, ScanArgs args) {
        Set<RedisClusterNode> nodes = connection.sync().nodes(n -> n.getRole().isMaster()).asMap().keySet();
        List<Flux<K>> flux = nodes.stream().map(n -> ScanStream.scan(connection.getConnection(n.getNodeId()).reactive(), args))
                .collect(Collectors.toList());
        return Flux.merge(flux).toIterable().iterator();
    }

    public static <K, V> ScanIterator<K> scanIterator(StatefulRedisConnection<K, V> connection, ScanArgs args) {
        return ScanIterator.scan(connection.sync(), args);
    }

    public static <K> Function<String, K> stringKeyFunction(RedisCodec<K, ?> codec) {
        Function<String, ByteBuffer> encode = StringCodec.UTF8::encodeKey;
        return encode.andThen(codec::decodeKey);
    }

    public static <K> Function<K, String> toStringKeyFunction(RedisCodec<K, ?> codec) {
        Function<K, ByteBuffer> encode = codec::encodeKey;
        return encode.andThen(StringCodec.UTF8::decodeKey);
    }

    public static <V> Function<String, V> stringValueFunction(RedisCodec<?, V> codec) {
        Function<String, ByteBuffer> encode = StringCodec.UTF8::encodeValue;
        return encode.andThen(codec::decodeValue);
    }

    public static <V> Function<V, String> toStringValueFunction(RedisCodec<?, V> codec) {
        Function<V, ByteBuffer> encode = codec::encodeValue;
        return encode.andThen(StringCodec.UTF8::decodeValue);
    }

    public static <V> Function<V, byte[]> toByteArrayValueFunction(RedisCodec<?, V> codec) {
        Function<V, ByteBuffer> encode = codec::encodeValue;
        return encode.andThen(ByteArrayCodec.INSTANCE::decodeValue);
    }

    public static <K> Function<K, byte[]> toByteArrayKeyFunction(RedisCodec<K, ?> codec) {
        Function<K, ByteBuffer> encode = codec::encodeKey;
        return encode.andThen(ByteArrayCodec.INSTANCE::decodeKey);
    }

    public static Range range(int value) {
        return new Range(value, value);
    }

    public static Range range(int min, int max) {
        return new Range(min, max);
    }

    public static <K, V, I, O> List<RedisFuture<O>> execute(RedisAsyncCommands<K, V> commands, List<? extends I> items,
            BiFunction<RedisAsyncCommands<K, V>, I, RedisFuture<O>> function) {
        List<RedisFuture<O>> futures = new ArrayList<>();
        for (I item : items) {
            futures.add(function.apply(commands, item));
        }
        return futures;
    }

    public static <T> List<T> getAll(Duration timeout, Iterable<RedisFuture<T>> futures) {
        try {
            List<T> items = new ArrayList<>();
            long nanos = timeout.toNanos();
            long time = System.nanoTime();
            for (RedisFuture<T> f : futures) {
                if (timeout.isNegative()) {
                    items.add(f.get());
                } else {
                    if (nanos < 0) {
                        throw new RedisCommandTimeoutException(String.format("Timed out after %s", timeout));
                    }
                    T item = f.get(nanos, TimeUnit.NANOSECONDS);
                    items.add(item);
                    long now = System.nanoTime();
                    nanos -= now - time;
                    time = now;
                }
            }
            return items;
        } catch (TimeoutException e) {
            throw new RedisCommandTimeoutException(e.getMessage());
        } catch (Exception e) {
            throw Exceptions.fromSynchronization(e);
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Set<T> asSet(T... elements) {
        return new HashSet<>(Arrays.asList(elements));
    }

    /**
     * x86 systems are little-endian and Java defaults to big-endian. This causes mismatching query results when RediSearch is
     * running in a x86 system. This method helps to convert concerned arrays.
     *
     * @param input float array
     * @return byte array
     */
    public static byte[] toByteArray(float[] input) {
        byte[] bytes = new byte[Float.BYTES * input.length];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(input);
        return bytes;
    }

    public static <K> String keyToString(K key) {
        if (key instanceof byte[]) {
            return new String((byte[]) key, StandardCharsets.UTF_8);
        }
        return String.valueOf(key);
    }

    public static <K> boolean keyEquals(K key1, K key2) {
        if (key1 == null) {
            return key2 == null;
        }
        if (key1 instanceof byte[] && key2 instanceof byte[]) {
            return Arrays.equals((byte[]) key1, (byte[]) key2);
        }
        return key1.equals(key2);
    }

    public static <K> int keyHashCode(K key) {
        if (key instanceof byte[]) {
            return Arrays.hashCode((byte[]) key);
        }
        return key.hashCode();
    }

}
