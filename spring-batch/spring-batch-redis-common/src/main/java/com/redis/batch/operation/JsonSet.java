package com.redis.batch.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.json.JsonParser;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.json.JsonValue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

public class JsonSet<K, V, T> extends AbstractValueWrite<K, V, V, T> {

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
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return execute(commands, items, this::execute);
    }

    @SuppressWarnings("rawtypes")
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        return commands.jsonSet(key(item), path(item), jsonValue(commands.getJsonParser(), value(item)));
    }

    private JsonValue jsonValue(JsonParser parser, V value) {
        if (value instanceof byte[]) {
            return parser.createJsonValue(ByteBuffer.wrap((byte[]) value));
        }
        return parser.createJsonValue((String) value);
    }

}
