package com.redis.riot.core;

import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.batch.operation.Xadd;
import com.redis.spring.batch.test.AbstractTestBase;
import com.redis.spring.batch.test.RedisContainerFactory;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.StreamMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.ListItemReader;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class CoreTests extends AbstractTestBase {

    private static final RedisServer redis = RedisContainerFactory.redis();

    @Override
    protected RedisServer getRedisServer() {
        return redis;
    }

    @Test
    void streamBackpressure(TestInfo info) throws Exception {
        String stream = "mystream";
        StreamLengthBackpressureStatusSupplier supplier = new StreamLengthBackpressureStatusSupplier(redisConnection, stream);
        supplier.setLimit(10);
        Assertions.assertFalse(supplier.get().shouldApplyBackpressure());
        IntStream.range(1, 12).forEach(index -> redisConnection.sync().xadd(stream, "field", "value"));
        BackpressureStatus status = supplier.get();
        Assertions.assertTrue(status.shouldApplyBackpressure());
        Assertions.assertEquals("Stream mystream reached max length: 11 > 10", status.getReason());
    }

    @Test
    void streamBackpressureWriter(TestInfo info) throws Exception {
        String stream = "mystream";
        ListItemReader<Integer> reader = new ListItemReader<>(IntStream.range(1, 100).boxed().toList());
        CompositeItemWriter<Integer> writer = new CompositeItemWriter<>();
        Xadd<String, String, Integer> xadd = new Xadd<>(t -> stream,
                t -> Arrays.asList(new StreamMessage<>(stream, null, map(t))));
        StreamLengthBackpressureStatusSupplier supplier = new StreamLengthBackpressureStatusSupplier(redisConnection, stream);
        supplier.setLimit(3);
        BackpressureItemWriter<Integer> backpressureWriter = new BackpressureItemWriter<>();
        backpressureWriter.setStatusSupplier(supplier);
        RedisItemWriter<String, String, Integer> streamWriter = RedisItemWriter.operation(xadd);
        streamWriter.setClient(redisClient);
        writer.setDelegates(Arrays.asList(backpressureWriter, streamWriter));
        SimpleStepBuilder<Integer, Integer> step = step(info, reader, null, writer);
        Job job = job(info).start(step.build()).build();
        JobExecution execution = jobLauncher.run(job, new JobParameters());
        Assertions.assertInstanceOf(BackpressureException.class, execution.getAllFailureExceptions().get(0));
    }

    private static Map<String, String> map(Integer integer) {
        Map<String, String> map = new HashMap<>();
        map.put("field", "value");
        return map;
    }

}
