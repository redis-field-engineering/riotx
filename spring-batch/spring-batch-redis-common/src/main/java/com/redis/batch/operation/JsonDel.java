package com.redis.batch.operation;

import com.redis.batch.BatchUtils;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.json.JsonPath;

import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

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
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return execute(commands, items, this::execute);
    }

    @SuppressWarnings("rawtypes")
    private RedisFuture execute(RedisAsyncCommands<K, V> commands, T item) {
        return commands.jsonDel(key(item), path(item));
    }

}
