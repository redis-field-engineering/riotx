package com.redis.riot.file.json;

import com.redis.riot.file.AbstractWriterFactory;
import com.redis.riot.file.WriteOptions;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.core.io.WritableResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.spring.batch.resource.JsonFileItemWriterBuilder;

public class JsonWriterFactory extends AbstractWriterFactory {

    @Override
    public ItemWriter<?> create(WritableResource resource, WriteOptions options) {
        JsonFileItemWriterBuilder<?> writer = new JsonFileItemWriterBuilder<>();
        writer.name(resource.getFilename());
        writer.resource(resource);
        writer.append(options.isAppend());
        writer.encoding(options.getEncoding().name());
        writer.forceSync(options.isForceSync());
        writer.lineSeparator(options.getLineSeparator());
        writer.saveState(false);
        writer.shouldDeleteIfEmpty(options.isShouldDeleteIfEmpty());
        writer.shouldDeleteIfExists(options.isShouldDeleteIfExists());
        writer.transactional(options.isTransactional());
        ObjectMapper objectMapper = objectMapper(new ObjectMapper(), options);
        writer.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>(objectMapper));
        return writer.build();
    }

}
