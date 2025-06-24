package com.redis.riot;

import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.reader.RedisLiveItemReader;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;

import lombok.ToString;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.util.function.Predicate;

@ToString
public class RedisReaderArgs {

    public static final long DEFAULT_SCAN_COUNT = RedisScanItemReader.DEFAULT_SCAN_LIMIT;

    public static final int DEFAULT_EVENT_QUEUE_CAPACITY = RedisLiveItemReader.DEFAULT_QUEUE_CAPACITY;

    public static final int DEFAULT_BATCH_SIZE = RedisScanItemReader.DEFAULT_BATCH_SIZE;

    @Option(names = "--key-pattern", defaultValue = "${RIOT_KEY_PATTERN}", description = "Pattern of keys to read (default: all keys).", paramLabel = "<glob>")
    private String keyPattern;

    @Option(names = "--key-type", defaultValue = "${RIOT_KEY_TYPE}", description = "Type of keys to read (default: all types).", paramLabel = "<type>")
    private String keyType;

    @Option(names = "--scan-count", defaultValue = "${RIOT_SCAN_COUNT:-100}", description = "How many keys to read at once on each scan iteration (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private long scanCount = DEFAULT_SCAN_COUNT;

    @Option(names = "--read-batch", defaultValue = "${RIOT_READ_BATCH:-50}", description = "Number of values each reader thread should read in a pipelined call (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int batchSize = DEFAULT_BATCH_SIZE;

    @Option(names = "--event-queue", defaultValue = "${RIOT_EVENT_QUEUE:-10000}", description = "Capacity of the keyspace notification queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int eventQueueCapacity = DEFAULT_EVENT_QUEUE_CAPACITY;

    @ArgGroup(exclusive = false)
    private KeyFilterArgs keyFilterArgs = new KeyFilterArgs();

    public <K, V> void configure(RedisItemReader<K, V, ?> reader) {
        reader.setBatchSize(batchSize);
        Predicate<K> keyPredicate = keyFilterArgs.predicate(reader.getCodec());
        reader.setKeyEventFilter(e -> keyPredicate.test(e.getKey()));
        reader.setKeyPattern(keyPattern);
        reader.setKeyType(keyType);
        if (reader instanceof RedisLiveItemReader) {
            ((RedisLiveItemReader<K, V, ?>) reader).setQueueCapacity(eventQueueCapacity);
        }
    }

    public String getKeyPattern() {
        return keyPattern;
    }

    public void setKeyPattern(String scanMatch) {
        this.keyPattern = scanMatch;
    }

    public long getScanCount() {
        return scanCount;
    }

    public void setScanCount(long scanCount) {
        this.scanCount = scanCount;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String scanType) {
        this.keyType = scanType;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int chunkSize) {
        this.batchSize = chunkSize;
    }

    public KeyFilterArgs getKeyFilterArgs() {
        return keyFilterArgs;
    }

    public void setKeyFilterArgs(KeyFilterArgs keyFilterArgs) {
        this.keyFilterArgs = keyFilterArgs;
    }

    public int getEventQueueCapacity() {
        return eventQueueCapacity;
    }

    public void setEventQueueCapacity(int eventQueueCapacity) {
        this.eventQueueCapacity = eventQueueCapacity;
    }

}
