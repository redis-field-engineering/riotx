package com.redis.batch.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.search.Suggestion;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;

public class Sugadd<K, V, T> extends AbstractValueWrite<K, V, Suggestion<V>, T> {

    private Predicate<T> incrPredicate = t -> false;

    public Sugadd(Function<T, K> keyFunction, Function<T, Suggestion<V>> valueFunction) {
        super(keyFunction, valueFunction);
    }

    public void setIncr(boolean incr) {
        this.incrPredicate = t -> incr;
    }

    public void setIncrPredicate(Predicate<T> predicate) {
        this.incrPredicate = predicate;
    }

    @Override
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return execute(commands, items, this::execute);
    }

    @SuppressWarnings("rawtypes")
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        if (incrPredicate.test(item)) {
            return ((RedisModulesAsyncCommands<K, V>) commands).ftSugaddIncr(key(item), value(item));
        }
        return ((RedisModulesAsyncCommands<K, V>) commands).ftSugadd(key(item), value(item));
    }

}
