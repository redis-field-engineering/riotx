package com.redis.riot.core.function;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

public class MapFilteringFunction<T> implements UnaryOperator<Map<String, T>> {

    private Collection<String> includes;

    private Collection<String> excludes;

    public MapFilteringFunction<T> excludes(Collection<String> fields) {
        this.excludes = new HashSet<>(fields);
        return this;
    }

    public MapFilteringFunction<T> includes(Collection<String> fields) {
        includes = new HashSet<>(fields);
        return this;
    }

    @Override
    public Map<String, T> apply(Map<String, T> source) {
        Map<String, T> filtered = ObjectUtils.isEmpty(includes) ? source : new LinkedHashMap<>();
        if (!CollectionUtils.isEmpty(includes)) {
            includes.forEach(f -> filtered.put(f, source.get(f)));
        }
        if (!CollectionUtils.isEmpty(excludes)) {
            excludes.forEach(filtered::remove);
        }
        return filtered;
    }

}
