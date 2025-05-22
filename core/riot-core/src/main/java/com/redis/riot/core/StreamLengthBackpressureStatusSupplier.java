package com.redis.riot.core;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import java.util.function.Supplier;

public class StreamLengthBackpressureStatusSupplier implements Supplier<BackpressureStatus> {

    public static final long DEFAULT_LIMIT = 10000;

    private final StatefulRedisModulesConnection<String, String> connection;

    private final String stream;

    private long limit = DEFAULT_LIMIT;

    public StreamLengthBackpressureStatusSupplier(StatefulRedisModulesConnection<String, String> connection, String stream) {
        this.connection = connection;
        this.stream = stream;
    }

    @Override
    public BackpressureStatus get() {
        Long length = connection.sync().xlen(stream);
        if (length != null && length > limit) {
            return BackpressureStatus.apply(String.format("Stream %s reached max length: %,d > %,d", stream, length, limit));
        }
        return BackpressureStatus.proceed();
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

}
