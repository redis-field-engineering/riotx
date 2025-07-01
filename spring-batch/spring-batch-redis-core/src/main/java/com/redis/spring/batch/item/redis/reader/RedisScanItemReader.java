package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.*;
import com.redis.batch.operation.AbstractKeyValueRead;
import com.redis.batch.operation.KeyDumpRead;
import com.redis.batch.operation.KeyStatRead;
import com.redis.batch.operation.KeyStructRead;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.spring.batch.item.redis.RedisItemReader;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.ToString;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@ToString
public class RedisScanItemReader<K, V, T> extends RedisItemReader<K, V, T> {

    public static final String SCAN_EVENT = "scan";

    public static final long DEFAULT_SCAN_LIMIT = 100;

    private long scanLimit = DEFAULT_SCAN_LIMIT;

    private Iterator<K> keyIterator;

    private Iterator<T> itemIterator = Collections.emptyIterator();

    public RedisScanItemReader(RedisCodec<K, V> codec, RedisBatchOperation<K, V, KeyEvent<K>, T> operation) {
        super(codec, operation);
    }

    public static RedisScanItemReader<byte[], byte[], KeyDumpEvent<byte[]>> dump() {
        return new RedisScanItemReader<>(ByteArrayCodec.INSTANCE, new KeyDumpRead());
    }

    public static RedisScanItemReader<String, String, KeyStatEvent<String>> stats() {
        return new RedisScanItemReader<>(StringCodec.UTF8, new KeyStatRead<>());
    }

    public static <K, V> RedisScanItemReader<K, V, KeyStructEvent<K, V>> struct(RedisCodec<K, V> codec) {
        return new RedisScanItemReader<>(codec, new KeyStructRead<>(codec));
    }

    private KeyScanArgs keyScanArgs() {
        KeyScanArgs args = new KeyScanArgs();
        args.limit(scanLimit);
        if (keyPattern != null) {
            args.match(keyPattern);
        }
        if (keyType != null) {
            args.type(keyType);
        }
        return args;
    }

    private Iterator<K> keyIterator() {
        return BatchUtils.scanIterator(ConnectionBuilder.client(client).readFrom(readFrom).connection(codec), keyScanArgs());
    }

    public List<T> fetch(int count) throws Exception {
        List<KeyEvent<K>> keys = new ArrayList<>();
        while (keys.size() < count && keyIterator.hasNext()) {
            K key = keyIterator.next();
            KeyEvent<K> keyEvent = new KeyEvent<>();
            keyEvent.setKey(key);
            keyEvent.setEvent(SCAN_EVENT);
            keyEvent.setTimestamp(Instant.now());
            keyEvent.setOperation(KeyOperation.READ);
            keys.add(keyEvent);
        }
        return read(keys);
    }

    @Override
    protected synchronized void doOpen() throws Exception {
        if (keyIterator == null) {
            keyIterator = keyIterator();
        }
        super.doOpen();
    }

    @Override
    protected synchronized T doRead() throws Exception {
        if (itemIterator.hasNext()) {
            return itemIterator.next();
        }
        List<T> keyValueEvents = fetch(batchSize);
        if (keyValueEvents.isEmpty()) {
            return null;
        }
        itemIterator = keyValueEvents.iterator();
        return itemIterator.next();
    }

    public long getScanLimit() {
        return scanLimit;
    }

    public void setScanLimit(long limit) {
        this.scanLimit = limit;
    }

}
