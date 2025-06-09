package com.redis.spring.batch.item.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.batch.gen.Generator;
import com.redis.spring.batch.item.AbstractCountingItemReader;
import com.redis.batch.KeyValue;

public class GeneratorItemReader extends AbstractCountingItemReader<KeyValue<String>> {

    private final Generator generator;

    public GeneratorItemReader(Generator generator) {
        this.generator = generator;
    }

    public Generator getGenerator() {
        return generator;
    }

    @Override
    protected void doOpen() {
        generator.reset();
    }

    @Override
    protected void doClose() {
        // do nothing
    }

    @Override
    protected KeyValue<String> doRead() throws JsonProcessingException {
        return generator.next();
    }

}
