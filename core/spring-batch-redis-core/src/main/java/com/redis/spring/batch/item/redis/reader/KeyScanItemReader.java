package com.redis.spring.batch.item.redis.reader;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.spring.batch.BatchRedisMetrics;
import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.ScanStream;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import reactor.core.publisher.Flux;

public class KeyScanItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<KeyEvent<K>> {

    public static final String COUNTER_NAME = "key.scan";

    public static final String COUNTER_DESCRIPTION = "Number of keys scanned";

    public static final String SCAN_EVENT = "scan";

    private final AbstractRedisClient client;

    private final RedisCodec<K, V> codec;

    private ReadFrom readFrom;

    private KeyScanArgs scanArgs = new KeyScanArgs();

    private StatefulRedisModulesConnection<K, V> connection;

    private Iterator<K> iterator;

    private MeterRegistry meterRegistry = Metrics.globalRegistry;

    private Counter counter;

    public KeyScanItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        setName(ClassUtils.getShortName(getClass()));
        this.client = client;
        this.codec = codec;
    }

    @Override
    protected synchronized void doOpen() throws Exception {
        if (counter == null) {
            counter = BatchRedisMetrics.createCounter(meterRegistry, COUNTER_NAME, COUNTER_DESCRIPTION,
                    Tag.of("name", getName()));
        }
        if (connection == null) {
            connection = BatchUtils.connection(client, codec, readFrom);
        }
        if (iterator == null) {
            iterator = iterator();
        }
    }

    private Iterator<K> iterator() {
        if (client instanceof RedisModulesClusterClient) {
            StatefulRedisModulesClusterConnection<K, V> clusterConnection = (StatefulRedisModulesClusterConnection<K, V>) connection;
            Set<RedisClusterNode> nodes = clusterConnection.sync().nodes(n -> n.getRole().isMaster()).asMap().keySet();
            List<Flux<K>> flux = nodes.stream()
                    .map(n -> ScanStream.scan(clusterConnection.getConnection(n.getNodeId()).reactive(), scanArgs))
                    .collect(Collectors.toList());
            return Flux.merge(flux).toIterable().iterator();
        }
        return ScanIterator.scan(connection.sync(), scanArgs);
    }

    @Override
    protected synchronized void doClose() throws Exception {
        iterator = null;
        if (connection != null) {
            connection.close();
            connection = null;
        }
        if (counter != null) {
            counter.close();
            counter = null;
        }
    }

    @Override
    protected synchronized KeyEvent<K> doRead() {
        if (iterator.hasNext()) {
            KeyEvent<K> key = new KeyEvent<>();
            key.setKey(iterator.next());
            key.setEvent(SCAN_EVENT);
            counter.increment();
            return key;
        }
        return null;
    }

    public KeyScanArgs getScanArgs() {
        return scanArgs;
    }

    public void setScanArgs(KeyScanArgs args) {
        this.scanArgs = args;
    }

    public ReadFrom getReadFrom() {
        return readFrom;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public void setMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

}
