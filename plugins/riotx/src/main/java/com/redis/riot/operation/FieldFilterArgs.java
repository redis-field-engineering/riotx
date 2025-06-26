package com.redis.riot.operation;

import com.redis.batch.BatchUtils;
import com.redis.riot.core.function.MapFilteringFunction;
import com.redis.riot.core.function.MapFlatteningFunction;
import lombok.ToString;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine.Option;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@ToString
public class FieldFilterArgs {

    @Option(arity = "1..*", names = "--include", description = "Fields to include.", paramLabel = "<field>")
    private List<String> includeFields;

    @Option(arity = "1..*", names = "--exclude", description = "Fields to exclude.", paramLabel = "<field>")
    private List<String> excludeFields;

    @Option(names = "--keep-null", description = "Keep fields with null values.")
    private boolean keepNull;

    @Option(names = "--keep-empty", description = "Keep fields with empty values.")
    private boolean keepEmpty;

    public Function<Map<String, Object>, Map<byte[], byte[]>> mapFunction() {
        MapFlatteningFunction flatten = new MapFlatteningFunction();
        flatten.setKeepNullValues(keepNull);
        flatten.setKeepEmptyValues(keepEmpty);
        if (ObjectUtils.isEmpty(includeFields) && ObjectUtils.isEmpty(excludeFields)) {
            return flatten.andThen(this::toByteArrayMap);
        }
        MapFilteringFunction<byte[]> filtering = new MapFilteringFunction<>();
        if (!ObjectUtils.isEmpty(includeFields)) {
            filtering.includes(includeFields);
        }
        if (!ObjectUtils.isEmpty(excludeFields)) {
            filtering.excludes(excludeFields);
        }
        return flatten.andThen(filtering).andThen(this::toByteArrayMap);
    }

    private Map<byte[], byte[]> toByteArrayMap(Map<String, byte[]> map) {
        Map<byte[], byte[]> result = new LinkedHashMap<>();
        map.forEach((k, v) -> result.put(BatchUtils.STRING_KEY_TO_BYTES.apply(k), v));
        return result;
    }

    public List<String> getExcludeFields() {
        return excludeFields;
    }

    public void setExcludeFields(List<String> excludes) {
        this.excludeFields = excludes;
    }

    public List<String> getIncludeFields() {
        return includeFields;
    }

    public void setIncludeFields(List<String> includes) {
        this.includeFields = includes;
    }

    public boolean isKeepNull() {
        return keepNull;
    }

    public void setKeepNull(boolean keepNull) {
        this.keepNull = keepNull;
    }

    public boolean isKeepEmpty() {
        return keepEmpty;
    }

    public void setKeepEmpty(boolean keepEmpty) {
        this.keepEmpty = keepEmpty;
    }

}
