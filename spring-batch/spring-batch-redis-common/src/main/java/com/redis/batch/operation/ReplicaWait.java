package com.redis.batch.operation;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.redis.batch.RedisBatchOperation;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;

public class ReplicaWait<K, V, T> extends AbstractCompositeWrite<K, V, T> {

    private final int replicas;

    private final long timeout;

    public ReplicaWait(RedisBatchOperation<K, V, T, Object> delegate, int replicas, Duration timeout) {
        super(delegate);
        this.replicas = replicas;
        this.timeout = timeout.toMillis();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public List<Future<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
        List<Future<Object>> futures = new ArrayList<>();
        futures.addAll(delegate.execute(commands, items));
        RedisFuture<Long> waitFuture = commands.waitForReplication(replicas, timeout);
        futures.add((RedisFuture) new PipelinedRedisFuture<>(waitFuture.thenAccept(this::checkReplicas)));
        return futures;
    }

    private void checkReplicas(Long actual) {
        if (actual == null || actual < replicas) {
            throw new RedisCommandExecutionException(errorMessage(actual));
        }
    }

    private String errorMessage(Long actual) {
        return MessageFormat.format("Insufficient replication level ({0}/{1})", actual, replicas);
    }

}
