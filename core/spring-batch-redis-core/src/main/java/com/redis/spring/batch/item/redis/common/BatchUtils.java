package com.redis.spring.batch.item.redis.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.batch.item.Chunk;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;

import com.hrakaroo.glob.GlobPattern;
import com.redis.spring.batch.BatchRedisMetrics;
import com.redis.spring.batch.item.redis.reader.KeyEvent;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.micrometer.core.instrument.Tags;

public abstract class BatchUtils {

	private BatchUtils() {
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <K, V, T, O> List<RedisFuture<O>> executeAll(RedisAsyncCommands<K, V> commands,
			Chunk<? extends T> items, BiFunction<RedisAsyncCommands<K, V>, T, RedisFuture<?>> function) {
		return (List) items.getItems().stream().map(item -> function.apply(commands, item))
				.collect(Collectors.toList());
	}

	public static String readFile(String filename) throws IOException {
		try (InputStream inputStream = BatchUtils.class.getClassLoader().getResourceAsStream(filename)) {
			return FileCopyUtils.copyToString(new InputStreamReader(inputStream));
		}
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

}
