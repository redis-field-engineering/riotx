package com.redis.spring.batch.item.redis.writer.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.json.JsonParser;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.json.JsonValue;

public class JsonSet<K, V, T> extends AbstractValueWriteOperation<K, V, V, T> {

    private Function<T, JsonPath> pathFunction = t -> JsonPath.ROOT_PATH;

    public JsonSet(Function<T, K> keyFunction, Function<T, V> valueFunction) {
        super(keyFunction, valueFunction);
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
        return ((RedisModulesAsyncCommands<K, V>) commands).jsonSet(key(item), path(item),
                jsonValue(commands.getJsonParser(), value(item)));
    }

    private JsonValue jsonValue(JsonParser parser, V value) {
        if (value instanceof byte[]) {
            return parser.createJsonValue(ByteBuffer.wrap((byte[]) value));
        }
        return parser.createJsonValue((String) value);
    }

}
