package com.redis.batch.operation;

import com.redis.batch.BatchUtils;
import com.redis.batch.MappingRedisFuture;
import com.redis.batch.RedisOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.List;
import java.util.function.BiFunction;

@SuppressWarnings("unchecked")
public class Evalsha<K, V, O> implements RedisOperation<K, V, K, O> {

    private final V[] args;

    private final BiFunction<K, List<Object>, O> function;

    private String digest;

    public Evalsha(BiFunction<K, List<Object>, O> function, V... args) {
        this.function = function;
        this.args = args;
    }

    @Override
    public List<RedisFuture<O>> execute(RedisAsyncCommands<K, V> commands, List<? extends K> items) {
        return BatchUtils.execute(commands, items, this::execute);
    }

    public RedisFuture<O> execute(RedisAsyncCommands<K, V> commands, K item) {
        K[] keys = (K[]) new Object[] { item };
        RedisFuture<List<Object>> future = commands.evalsha(digest, ScriptOutputType.MULTI, keys, args);
        return new MappingRedisFuture<>(future, s -> function.apply(item, s));
    }

    public String getDigest() {
        return digest;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }

}
