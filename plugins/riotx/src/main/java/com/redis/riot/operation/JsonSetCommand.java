package com.redis.riot.operation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redis.spring.batch.item.redis.writer.impl.JsonSet;
import io.lettuce.core.json.JsonPath;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.core.serializer.support.SerializationFailedException;
import org.springframework.util.StringUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.function.Function;

@Command(name = "json.set", description = "Add JSON documents to RedisJSON")
public class JsonSetCommand extends AbstractOperationCommand {

    private final ObjectMapper mapper = new ObjectMapper();

    private final ObjectWriter jsonWriter = mapper.writerFor(Map.class);

    @Option(names = "--path", description = "JSON path (default: ${DEFAULT-VALUE}).", paramLabel = "<field>")
    private JsonPath path = JsonPath.ROOT_PATH;

    @Option(names = "--path-field", description = "Name of field containing JSON path.", paramLabel = "<field>")
    private String pathField;

    @Option(names = "--value", description = "Specific field to use for value.", paramLabel = "<field>")
    private String valueField;

    @Override
    public JsonSet<String, String, Map<String, Object>> operation() {
        JsonSet<String, String, Map<String, Object>> operation = new JsonSet<>(keyFunction(), valueFunction());
        if (pathField == null) {
            operation.setPath(path);
        } else {
            operation.setPathFunction(toString(pathField).andThen(this::jsonPath));
        }
        return operation;
    }

    private Function<Map<String, Object>, String> valueFunction() {
        if (StringUtils.hasLength(valueField)) {
            return this::fieldValue;
        }
        return this::jsonValue;
    }

    private String fieldValue(Map<String, Object> map) {
        Object value = map.get(valueField);
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new SerializationFailedException(
                    String.format("Could not serialize field %s value '%s' to JSON", valueField, value), e);
        }
    }

    private JsonPath jsonPath(String path) {
        if (StringUtils.hasLength(path)) {
            return JsonPath.of(path);
        }
        return JsonPath.ROOT_PATH;
    }

    private String jsonValue(Map<String, Object> map) {
        try {
            return jsonWriter.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new ItemStreamException("Could not serialize to JSON", e);
        }
    }

    public JsonPath getPath() {
        return path;
    }

    public void setPath(JsonPath path) {
        this.path = path;
    }

    public String getPathField() {
        return pathField;
    }

    public void setPathField(String pathField) {
        this.pathField = pathField;
    }

}
