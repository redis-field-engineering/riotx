package com.redis.spring.batch.item.redis.common;

import com.hrakaroo.glob.GlobPattern;
import com.redis.spring.batch.BatchRedisMetrics;
import com.redis.spring.batch.item.redis.reader.KeyEvent;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import com.redis.spring.batch.item.redis.reader.RedisScanSizeEstimator;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.ScanStream;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.micrometer.core.instrument.Tags;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class BatchUtils {

    private BatchUtils() {
    }

    public static String readFile(String filename) throws IOException {
        try (InputStream inputStream = BatchUtils.class.getClassLoader().getResourceAsStream(filename)) {
            return FileCopyUtils.copyToString(new InputStreamReader(inputStream));
        }
    }

    public static <K, V> LongSupplier scanSizeEstimator(RedisScanItemReader<K, V> reader) {
        return RedisScanSizeEstimator.from(reader.getClient(), reader.getKeyPattern(), reader.getKeyType());
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

    public static <T> Stream<T> stream(Iterable<T> items) {
        return StreamSupport.stream(items.spliterator(), false);
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

    public static <K> String toString(K key) {
        if (key instanceof byte[]) {
            return new String((byte[]) key, StandardCharsets.UTF_8);
        }
        return String.valueOf(key);
    }

    public static Predicate<String> globPredicate(String pattern) {
        if (StringUtils.hasLength(pattern)) {
            return GlobPattern.compile(pattern)::matches;
        }
        return t -> true;
    }

    public static Tags tags(KeyEvent<?> item, String status) {
        return Tags.of("event", BatchRedisMetrics.tagValue(item.getEvent()), "status", status, "type",
                BatchRedisMetrics.tagValue(item.getType()));
    }

    public static <K, V, I, O> List<RedisFuture<O>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends I> items,
            BiFunction<RedisAsyncCommands<K, V>, I, RedisFuture<O>> function) {
        List<RedisFuture<O>> futures = new ArrayList<>();
        for (I item : items) {
            futures.add(function.apply(commands, item));
        }
        return futures;
    }

    public static <T> List<T> getAll(Duration timeout, Iterable<RedisFuture<T>> futures)
            throws TimeoutException, InterruptedException, ExecutionException {
        List<T> items = new ArrayList<>();
        long nanos = timeout.toNanos();
        long time = System.nanoTime();
        for (RedisFuture<T> f : futures) {
            if (timeout.isNegative()) {
                items.add(f.get());
            } else {
                if (nanos < 0) {
                    throw new TimeoutException(String.format("Timed out after %s", timeout));
                }
                T item = f.get(nanos, TimeUnit.NANOSECONDS);
                items.add(item);
                long now = System.nanoTime();
                nanos -= now - time;
                time = now;
            }
        }
        return items;
    }

}
