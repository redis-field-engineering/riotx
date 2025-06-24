package com.redis.spring.batch.item.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.batch.KeyValueEvent;
import com.redis.batch.gen.Generator;
import com.redis.spring.batch.item.AbstractCountingItemReader;

public class GeneratorItemReader extends AbstractCountingItemReader<KeyValueEvent<String>> {

    private final Generator generator;

    public GeneratorItemReader(Generator generator) {
        this.generator = generator;
    }

    public Generator getGenerator() {
        return generator;
    }

    @Override
    protected void doOpen() {
        generator.setCurrentIndex(getCurrentItemCount());
    }

    @Override
    protected void doClose() {
        // do nothing
    }

    @Override
    protected KeyValueEvent<String> doRead() throws JsonProcessingException {
        return generator.next();
    }

}
