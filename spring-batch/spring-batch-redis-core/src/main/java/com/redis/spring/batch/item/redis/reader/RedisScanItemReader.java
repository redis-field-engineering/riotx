package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.*;
import com.redis.batch.operation.KeyValueRead;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.spring.batch.item.redis.RedisItemReader;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@ToString
public class RedisScanItemReader<K, V> extends RedisItemReader<K, V> {

    public static final String SCAN_EVENT = "scan";

    public static final long DEFAULT_SCAN_LIMIT = 100;

    private long scanLimit = DEFAULT_SCAN_LIMIT;

    private Iterator<K> keyIterator;

    private Iterator<KeyValue<K>> valueIterator = Collections.emptyIterator();

    public RedisScanItemReader(RedisCodec<K, V> codec, RedisOperation<K, V, K, KeyValue<K>> operation) {
        super(codec, operation);
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

    private KeyValue<K> keyEvent(K key) {
        KeyValue<K> keyEvent = new KeyValue<>();
        keyEvent.setKey(key);
        keyEvent.setEvent(SCAN_EVENT);
        return keyEvent;
    }

    private Iterator<K> keyIterator() {
        return BatchUtils.scanIterator(ConnectionBuilder.client(client).readFrom(readFrom).connection(codec), keyScanArgs());
    }

    public List<KeyValue<K>> fetch(int count) throws Exception {
        List<KeyValue<K>> keys = new ArrayList<>();
        while (keys.size() < count && keyIterator.hasNext()) {
            K key = keyIterator.next();
            if (acceptKey(key)) {
                KeyValue<K> keyEvent = keyEvent(key);
                keys.add(keyEvent);
            }
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
    protected synchronized KeyValue<K> doRead() throws Exception {
        if (valueIterator.hasNext()) {
            return valueIterator.next();
        }
        List<KeyValue<K>> keyValues = fetch(batchSize);
        if (keyValues.isEmpty()) {
            return null;
        }
        valueIterator = keyValues.iterator();
        return valueIterator.next();
    }


    public static RedisScanItemReader<byte[], byte[]> dump() {
        return new RedisScanItemReader<>(ByteArrayCodec.INSTANCE, KeyValueRead.dump(ByteArrayCodec.INSTANCE));
    }

    public static RedisScanItemReader<String, String> struct() {
        return struct(StringCodec.UTF8);
    }

    public static <K, V> RedisScanItemReader<K, V> struct(RedisCodec<K, V> codec) {
        return new RedisScanItemReader<>(codec, KeyValueRead.struct(codec));
    }

    public static RedisScanItemReader<String, String> type() {
        return type(StringCodec.UTF8);
    }

    public static <K, V> RedisScanItemReader<K, V> type(RedisCodec<K, V> codec) {
        return new RedisScanItemReader<>(codec, KeyValueRead.type(codec));
    }

    public long getScanLimit() {
        return scanLimit;
    }

    public void setScanLimit(long limit) {
        this.scanLimit = limit;
    }

}
