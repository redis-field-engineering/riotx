package com.redis.spring.batch.item.redis.reader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.internal.Exceptions;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RedisScanSizeEstimator implements LongSupplier {

    public static final int DEFAULT_SAMPLES = 100;

    private AbstractRedisClient client;

    private int samples = DEFAULT_SAMPLES;

    private String keyPattern;

    private String keyType;

    /**
     * Estimates the number of keys that match the given pattern and type.
     * 
     * @return Estimated number of keys matching the given pattern and type, or null if database is empty or any error occurs
     */
    @Override
    public long getAsLong() {
        Assert.notNull(client, "Redis client not set");
        try (StatefulRedisModulesConnection<String, String> connection = ConnectionBuilder.client(client).connection()) {
            try {
                return size(connection);
            } catch (Exception e) {
                throw Exceptions.bubble(e);
            } finally {
                connection.setAutoFlushCommands(true);
            }
        }
    }

    private long size(StatefulRedisModulesConnection<String, String> connection)
            throws TimeoutException, InterruptedException, ExecutionException {
        RedisModulesAsyncCommands<String, String> commands = connection.async();
        long dbsize = connection.sync().dbsize();
        if (!StringUtils.hasLength(keyPattern) && !StringUtils.hasLength(keyType)) {
            return dbsize;
        }
        commands.getStatefulConnection().setAutoFlushCommands(false);
        List<RedisFuture<String>> keyFutures = new ArrayList<>();
        for (int index = 0; index < samples; index++) {
            keyFutures.add(commands.randomkey());
        }
        commands.getStatefulConnection().flushCommands();
        List<String> keys = BatchUtils.getAll(connection.getTimeout(), keyFutures);
        List<RedisFuture<String>> typeFutures = keys.stream().map(commands::type).collect(Collectors.toList());
        connection.flushCommands();
        List<String> types = BatchUtils.getAll(connection.getTimeout(), typeFutures);
        Map<String, String> keyTypes = new HashMap<>();
        for (int index = 0; index < keys.size(); index++) {
            keyTypes.put(keys.get(index), types.get(index));
        }
        Predicate<String> glob = BatchUtils.globPredicate(keyPattern);
        Predicate<String> type = t -> keyType == null || keyType.equalsIgnoreCase(t);
        long matches = keyTypes.entrySet().stream().filter(e -> glob.test(e.getKey()) && type.test(e.getValue())).count();
        return Math.round(dbsize * (double) matches / keyTypes.size());
    }

    public AbstractRedisClient getClient() {
        return client;
    }

    public void setClient(AbstractRedisClient client) {
        this.client = client;
    }

    public String getKeyPattern() {
        return keyPattern;
    }

    public void setKeyPattern(String keyPattern) {
        this.keyPattern = keyPattern;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }

    public int getSamples() {
        return samples;
    }

    public void setSamples(int samples) {
        this.samples = samples;
    }

    public static RedisScanSizeEstimator from(AbstractRedisClient client, String keyPattern, String keyType) {
        RedisScanSizeEstimator estimator = new RedisScanSizeEstimator();
        estimator.setClient(client);
        estimator.setKeyPattern(keyPattern);
        estimator.setKeyType(keyType);
        return estimator;
    }

}
