package com.redis.riot.core.function;

import com.redis.batch.KeyStructEvent;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

import java.util.*;
import java.util.function.Function;

public class KeyValueEventToMap implements Function<KeyStructEvent<String, String>, Map<String, Object>> {

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
    public Map<String, Object> apply(KeyStructEvent<String, String> item) {
        Map<String, Object> map = new LinkedHashMap<>(key.apply(item.getKey()));
        map.putAll(value(item));
        return map;
    }

    private Map<String, String> value(KeyStructEvent<String, String> item) {
        if (item.getValue() == null) {
            return Collections.emptyMap();
        }
        switch (item.getType()) {
            case none:
                return Collections.emptyMap();
            case hash:
                return hash.apply(item.asHash());
            case list:
                return list.apply(item.asList());
            case set:
                return set.apply(item.asSet());
            case zset:
                return zset.apply(item.asZSet());
            case stream:
                return stream.apply(item.asStream());
            case json:
                return json.apply(item.asJson());
            case string:
                return string.apply(item.asString());
            case timeseries:
                return timeseries.apply(item.asTimeseries());
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
