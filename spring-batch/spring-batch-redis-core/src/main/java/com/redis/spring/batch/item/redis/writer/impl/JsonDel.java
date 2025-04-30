package com.redis.spring.batch.item.redis.writer.impl;

import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.json.JsonPath;

public class JsonDel<K, V, T> extends AbstractWriteOperation<K, V, T> {

    private Function<T, JsonPath> pathFunction = t -> JsonPath.ROOT_PATH;

    public JsonDel(Function<T, K> keyFunction) {
        super(keyFunction);
    }

    public void setPath(JsonPath path) {
        this.pathFunction = t -> path;
    }

    public void setPathFunction(Function<T, JsonPath> path) {
        this.pathFunction = path;
    }

    private JsonPath path(T item) {
        return pathFunction.apply(item);
    }

    @Override
    public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
        return BatchUtils.execute(commands, items, this::execute);
    }

    @SuppressWarnings("rawtypes")
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        return commands.jsonDel(key(item), path(item));
    }

}
