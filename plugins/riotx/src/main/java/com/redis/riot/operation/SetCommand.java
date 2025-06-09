package com.redis.riot.operation;

import java.util.Map;
import java.util.function.Function;

import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.core.function.ObjectMapperFunction;
import com.redis.batch.operation.Set;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "set", description = "Set strings from input")
public class SetCommand extends AbstractOperationCommand {

    public enum StringFormat {
        XML, JSON
    }

    public static final StringFormat DEFAULT_FORMAT = StringFormat.JSON;

    @Option(names = "--format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<fmt>")
    private StringFormat format = DEFAULT_FORMAT;

    @Option(names = "--value", description = "Raw value field. Disables serialization.", paramLabel = "<field>")
    private String value;

    @Option(names = "--root", description = "XML root element name.", paramLabel = "<name>")
    private String root;

    @Override
    public Set<String, String, Map<String, Object>> operation() {
        return new Set<>(keyFunction(), value());
    }

    private Function<Map<String, Object>, String> value() {
        if (StringUtils.hasLength(value)) {
            return toString(value);
        }
        if (format == StringFormat.XML) {
            return new ObjectMapperFunction<>(new XmlMapper().writer().withRootName(root));
        }
        ObjectMapper jsonMapper = new ObjectMapper();
        jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        jsonMapper.setSerializationInclusion(Include.NON_NULL);
        return new ObjectMapperFunction<>(jsonMapper.writer().withRootName(root));
    }

    public StringFormat getFormat() {
        return format;
    }

    public void setFormat(StringFormat format) {
        this.format = format;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
    }

}
