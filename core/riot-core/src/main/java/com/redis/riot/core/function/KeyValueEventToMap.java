package com.redis.riot.core.function;

import com.redis.batch.KeyType;
import com.redis.batch.KeyValueEvent;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

import java.util.*;
import java.util.function.Function;

public class KeyValueEventToMap implements Function<KeyValueEvent<String>, Map<String, Object>> {

    private Function<String, Map<String, String>> key = t -> Collections.emptyMap();

    private Function<Map<String, String>, Map<String, String>> hash = new HashToMapFunction();

    private Function<Collection<Sample>, Map<String, String>> timeseries = new TimeSeriesToMapFunction();

    private Function<Collection<StreamMessage<String, String>>, Map<String, String>> stream = new StreamToMapFunction();

    private Function<Collection<String>, Map<String, String>> list = new CollectionToMapFunction();

    private Function<Collection<String>, Map<String, String>> set = new CollectionToMapFunction();

    private Function<Set<ScoredValue<String>>, Map<String, String>> zset = new ZsetToMapFunction();

    private Function<String, Map<String, String>> json = new StringToMapFunction();

    private Function<String, Map<String, String>> string = new StringToMapFunction();

    private Function<Object, Map<String, String>> defaultFunction = s -> Collections.emptyMap();

    @Override
    public Map<String, Object> apply(KeyValueEvent<String> item) {
        Map<String, Object> map = new LinkedHashMap<>(key.apply(item.getKey()));
        map.putAll(value(item));
        return map;
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> value(KeyValueEvent<String> item) {
        if (item.getValue() == null) {
            return Collections.emptyMap();
        }
        KeyType type = KeyType.of(item.getType());
        if (type == null) {
            return Collections.emptyMap();
        }
        switch (type) {
            case NONE:
                return Collections.emptyMap();
            case HASH:
                return hash.apply((Map<String, String>) item.getValue());
            case LIST:
                return list.apply((Collection<String>) item.getValue());
            case SET:
                return set.apply((Set<String>) item.getValue());
            case ZSET:
                return zset.apply((Set<ScoredValue<String>>) item.getValue());
            case STREAM:
                return stream.apply((Collection<StreamMessage<String, String>>) item.getValue());
            case JSON:
                return json.apply((String) item.getValue());
            case STRING:
                return string.apply((String) item.getValue());
            case TIMESERIES:
                return timeseries.apply((Collection<Sample>) item.getValue());
            default:
                return defaultFunction.apply(item.getValue());
        }
    }

    public void setKey(Function<String, Map<String, String>> key) {
        this.key = key;
    }

    public void setHash(Function<Map<String, String>, Map<String, String>> function) {
        this.hash = function;
    }

    public void setStream(Function<Collection<StreamMessage<String, String>>, Map<String, String>> function) {
        this.stream = function;
    }

    public void setList(Function<Collection<String>, Map<String, String>> function) {
        this.list = function;
    }

    public void setSet(Function<Collection<String>, Map<String, String>> function) {
        this.set = function;
    }

    public void setZset(Function<Set<ScoredValue<String>>, Map<String, String>> function) {
        this.zset = function;
    }

    public void setString(Function<String, Map<String, String>> function) {
        this.string = function;
    }

    public void setDefaultFunction(Function<Object, Map<String, String>> function) {
        this.defaultFunction = function;
    }

    public void setJson(Function<String, Map<String, String>> function) {
        this.json = function;
    }

    public void setTimeseries(Function<Collection<Sample>, Map<String, String>> function) {
        this.timeseries = function;
    }

}
