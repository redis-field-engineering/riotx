package com.redis.batch.operation;

import com.redis.batch.*;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class KeyStatRead<K, V> implements RedisBatchOperation<K, V, KeyEvent<K>, KeyStatEvent<K>> {

    @Override
    public List<RedisFuture<KeyStatEvent<K>>> execute(RedisAsyncCommands<K, V> commands, List<? extends KeyEvent<K>> items) {
        return items.stream().map(e -> execute(commands, e)).collect(Collectors.toList());
    }

    private RedisFuture<KeyStatEvent<K>> execute(RedisAsyncCommands<K, V> commands, KeyEvent<K> item) {
        // Execute Redis commands to get key information
        CompletableFuture<Long> memUsageFuture = commands.memoryUsage(item.getKey()).toCompletableFuture();
        CompletableFuture<Long> ttlFuture = commands.pttl(item.getKey()).toCompletableFuture();
        CompletableFuture<String> typeFuture = commands.type(item.getKey()).toCompletableFuture();

        // Wait for all futures to complete in parallel
        CompletableFuture<KeyStatEvent<K>> combinedFuture = CompletableFuture.allOf(memUsageFuture, ttlFuture, typeFuture)
                .thenApply(v -> {
                    KeyStatEvent<K> event = new KeyStatEvent<>();
                    event.setKey(item.getKey());
                    event.setMemoryUsage(memUsageFuture.join());
                    Long ttl = ttlFuture.join();
                    if (ttl != null && ttl > 0) {
                        event.setTtl(Instant.now().plusMillis(ttl));
                    }
                    event.setType(KeyType.of(typeFuture.join()));
                    return event;
                });
        return new MappingRedisFuture<>(combinedFuture, keyInfo -> keyInfo);
    }

}
