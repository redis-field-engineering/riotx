package com.redis.riot.core.function;

import com.redis.batch.BatchUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Flattens a nested map using . and [] notation for key names
 */
public class MapFlatteningFunction implements Function<Map<String, Object>, Map<String, byte[]>> {

    @Override
    public Map<String, byte[]> apply(Map<String, Object> source) {
        Map<String, byte[]> resultMap = new LinkedHashMap<>();
        flatten("", source.entrySet().iterator(), resultMap);
        return resultMap;
    }

    private void flatten(String prefix, Iterator<? extends Entry<String, Object>> map, Map<String, byte[]> flatMap) {
        String actualPrefix = StringUtils.hasText(prefix) ? prefix.concat(".") : prefix;
        while (map.hasNext()) {
            Entry<String, Object> element = map.next();
            flattenElement(actualPrefix.concat(element.getKey()), element.getValue(), flatMap);
        }
    }

    @SuppressWarnings("unchecked")
    private void flattenElement(String key, @Nullable Object value, Map<String, byte[]> map) {
        if (value == null) {
            return;
        }
        if (value instanceof Map) {
            flatten(key, ((Map<String, Object>) value).entrySet().iterator(), map);
        } else {
            map.put(key, elementBytes(value));
        }
    }

    private byte[] elementBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value.getClass().isArray()) {
            return toBytes(CollectionUtils.arrayToList(value));
        }
        if (value instanceof Iterable) {
            return toBytes((Iterable<?>) value);
        }
        return processElement(value);
    }

    private byte[] toBytes(Iterable<?> iterable) {
        return BatchUtils.STRING_KEY_TO_BYTES.apply(
                StreamSupport.stream(iterable.spliterator(), false).map(String::valueOf).collect(Collectors.joining(",")));
    }

    private byte[] processElement(Object value) {
        if (value == null) {
            return null;
        }
        // byte arrays should not be processed but passed to Redis as-is
        return BatchUtils.STRING_KEY_TO_BYTES.apply(String.valueOf(value));
    }

}
