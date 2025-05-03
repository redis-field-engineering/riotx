package com.redis.spring.batch.item.redis.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.RedisOperation;

import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.RedisCodec;
import lombok.ToString;

@ToString
public class RedisScanItemReader<K, V> extends RedisItemReader<K, V> {

    public static final String SCAN_EVENT = "scan";

    public static final long DEFAULT_SCAN_LIMIT = 100;

    private long scanLimit = DEFAULT_SCAN_LIMIT;

    private Iterator<K> keyIterator;

    private Iterator<KeyValue<K>> valueIterator = Collections.emptyIterator();

    public RedisScanItemReader(RedisCodec<K, V> codec, RedisOperation<K, V, KeyEvent<K>, KeyValue<K>> operation) {
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

    private KeyEvent<K> keyEvent(K key) {
        KeyEvent<K> keyEvent = new KeyEvent<>();
        keyEvent.setKey(key);
        keyEvent.setEvent(SCAN_EVENT);
        return keyEvent;
    }

    private Iterator<K> keyIterator() {
        return BatchUtils.scanIterator(ConnectionBuilder.client(client).readFrom(readFrom).connection(codec), keyScanArgs());
    }

    public List<KeyValue<K>> fetch(int count) throws Exception {
        List<KeyEvent<K>> keys = new ArrayList<>();
        while (keys.size() < count && keyIterator.hasNext()) {
            K key = keyIterator.next();
            if (acceptKey(key)) {
                KeyEvent<K> keyEvent = keyEvent(key);
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

    public long getScanLimit() {
        return scanLimit;
    }

    public void setScanLimit(long limit) {
        this.scanLimit = limit;
    }

}
