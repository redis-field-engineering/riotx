package com.redis.riot.file.json;

import com.redis.riot.file.AbstractWriterFactory;
import com.redis.riot.file.WriteOptions;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.io.WritableResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.spring.batch.resource.FlatFileItemWriterBuilder;

public class JsonLinesWriterFactory extends AbstractWriterFactory {

    @Override
    public ItemWriter<?> create(WritableResource resource, WriteOptions options) {
        FlatFileItemWriterBuilder<?> builder = flatFileWriter(resource, options);
        ObjectMapper objectMapper = objectMapper(new ObjectMapper(), options);
        builder.lineAggregator(new JsonLineAggregator<>(objectMapper));
        return builder.build();
    }

}
