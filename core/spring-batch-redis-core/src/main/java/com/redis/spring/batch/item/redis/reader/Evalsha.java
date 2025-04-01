package com.redis.spring.batch.item.redis.reader;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

@SuppressWarnings("unchecked")
public class Evalsha<K, V, I, O> implements Operation<K, V, I, O> {

	private static final Object[] EMPTY_ARRAY = new Object[0];

	private final Function<String, V> stringValueFunction;
	private final Function<I, K> keyFunction;
	private final BiFunction<I, List<Object>, O> valueFunction;

	private Function<I, V[]> argsFunction = t -> (V[]) EMPTY_ARRAY;
	private String digest;

	public Evalsha(RedisCodec<K, V> codec, Function<I, K> key, BiFunction<I, List<Object>, O> value) {
		this.stringValueFunction = BatchUtils.stringValueFunction(codec);
		this.keyFunction = key;
		this.valueFunction = value;
	}

	@Override
	public List<RedisFuture<O>> execute(RedisAsyncCommands<K, V> commands, Chunk<? extends I> items) {
		return items.getItems().stream().map(t -> execute(commands, t)).collect(Collectors.toList());
	}

	public RedisFuture<O> execute(RedisAsyncCommands<K, V> commands, I item) {
		K[] keys = (K[]) new Object[] { keyFunction.apply(item) };
		V[] args = argsFunction.apply(item);
		RedisFuture<List<Object>> output = commands.evalsha(digest, ScriptOutputType.MULTI, keys, args);
		return new MappingRedisFuture<>(output, l -> valueFunction.apply(item, l));
	}

	public void setArgsFunction(Function<I, V[]> function) {
		this.argsFunction = function;
	}

	public void setArgs(Object... args) {
		V[] encodedArgs = (V[]) new Object[args.length];
		for (int index = 0; index < args.length; index++) {
			encodedArgs[index] = stringValueFunction.apply(String.valueOf(args[index]));
		}
		this.argsFunction = t -> encodedArgs;
	}

	public String getDigest() {
		return digest;
	}

	public void setDigest(String digest) {
		this.digest = digest;
	}
}
