package com.redis.riotx;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.item.redis.common.Key;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyEvent;
import com.tdunning.math.stats.TDigest;

public class DatabaseStats {

    public static final String DEFAULT_KEYSPACE_REGEX = "^([^:]+):";

    private final Map<String, InternalKeyspace> keyspaces = new LinkedHashMap<>();

    private Function<Key<String>, String> keyspaceFunction = keyspaceFunction(DEFAULT_KEYSPACE_REGEX);

    private Predicate<KeyValue<String>> bigKeyPredicate = kv -> true;

    private InternalKeyspace keyspace(Key<String> key) {
        return keyspaces.computeIfAbsent(keyspaceFunction.apply(key), InternalKeyspace::new);
    }

    public void onKeyEvent(KeyEvent<String> keyEvent) {
        InternalKeyspace keyspace = keyspace(keyEvent);
        InternalBigKey bigKey = keyspace.getBigKeys().get(keyEvent.getKey());
        if (bigKey != null) {
            bigKey.incrementWrites();
        }
    }

    public synchronized void keyValue(KeyValue<String> kv) {
        InternalKeyspace keyspace = keyspace(kv);
        keyspace.addKeyValue(kv);
        if (bigKeyPredicate.test(kv)) {
            keyspace.addBigKey(bigKey(kv));
        }

    }

    private InternalBigKey bigKey(KeyValue<String> kv) {
        InternalBigKey bigKey = new InternalBigKey();
        bigKey.setKey(kv.getKey());
        bigKey.setMemoryUsage(DataSize.ofBytes(kv.getMemoryUsage()));
        bigKey.setType(kv.getType());
        return bigKey;
    }

    public void setKeyspaceRegex(String regex) {
        setKeyspaceFunction(keyspaceFunction(regex));
    }

    public void setKeyspacePattern(Pattern pattern) {
        setKeyspaceFunction(keyspaceFunction(pattern));
    }

    public void setKeyspaceFunction(Function<Key<String>, String> function) {
        this.keyspaceFunction = function;
    }

    public void setBigKeyPredicate(Predicate<KeyValue<String>> predicate) {
        this.bigKeyPredicate = predicate;
    }

    public List<Keyspace> keyspaces() {
        return keyspaces.values().stream().map(this::keyspace).collect(Collectors.toList());
    }

    private Keyspace keyspace(InternalKeyspace k) {
        Keyspace keyspace = new Keyspace();
        keyspace.setTypeCounts(
                k.getTypes().entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> e.getValue().get())));
        keyspace.setMemoryUsage(k.getMemoryUsage());
        keyspace.setPrefix(k.getPrefix());
        keyspace.setBigKeys(k.getBigKeys().size());
        return keyspace;
    }

    public synchronized List<BigKey> bigKeys() {
        return keyspaces.values().stream().flatMap(s -> s.getBigKeys().values().stream()).map(this::bigKey)
                .collect(Collectors.toList());
    }

    private BigKey bigKey(InternalBigKey internalKey) {
        BigKey key = new BigKey();
        key.setKey(internalKey.getKey());
        key.setMemoryUsage(internalKey.getMemoryUsage());
        key.setType(internalKey.getType());
        key.setWriteThroughput(internalKey.throughput());
        return key;
    }

    public static Function<Key<String>, String> keyspaceFunction(String regex) {
        return new KeyspaceFunction(regex);
    }

    public static Function<Key<String>, String> keyspaceFunction(Pattern pattern) {
        return new KeyspaceFunction(pattern);
    }

    public static class KeyspaceFunction implements Function<Key<String>, String> {

        private final Pattern pattern;

        public KeyspaceFunction(String pattern) {
            this(Pattern.compile(pattern));
        }

        public KeyspaceFunction(Pattern pattern) {
            this.pattern = pattern;
        }

        @Override
        public String apply(Key<String> t) {
            Matcher matcher = pattern.matcher(t.getKey());
            if (matcher.find()) {
                return matcher.group(1);
            }
            return t.getKey();
        }

    }

    public static class Keyspace {

        private String prefix;

        private Map<String, Integer> typeCounts;

        private int bigKeys;

        private TDigest memoryUsage;

        public String getPrefix() {
            return prefix;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        public Map<String, Integer> getTypeCounts() {
            return typeCounts;
        }

        public void setTypeCounts(Map<String, Integer> counts) {
            this.typeCounts = counts;
        }

        public int getBigKeys() {
            return bigKeys;
        }

        public void setBigKeys(int count) {
            this.bigKeys = count;
        }

        public TDigest getMemoryUsage() {
            return memoryUsage;
        }

        public void setMemoryUsage(TDigest memoryUsage) {
            this.memoryUsage = memoryUsage;
        }

    }

    /**
     * Aggregates statistics at the keyspace level
     */
    private static class InternalKeyspace {

        private final String prefix;

        private final Map<String, AtomicInteger> types = new HashMap<>();

        private final TDigest memoryUsage = TDigest.createDigest(100);

        private final Map<String, InternalBigKey> bigKeys = new HashMap<>();

        public InternalKeyspace(String prefix) {
            this.prefix = prefix;
        }

        public void addKeyValue(KeyValue<String> item) {
            types.computeIfAbsent(item.getType(), t -> new AtomicInteger()).incrementAndGet();
            if (item.getMemoryUsage() > 0) {
                memoryUsage.add(item.getMemoryUsage());
            }
        }

        public void addBigKey(InternalBigKey key) {
            bigKeys.put(key.getKey(), key);
        }

        public String getPrefix() {
            return prefix;
        }

        public Map<String, AtomicInteger> getTypes() {
            return types;
        }

        public TDigest getMemoryUsage() {
            return memoryUsage;
        }

        public Map<String, InternalBigKey> getBigKeys() {
            return bigKeys;
        }

    }

    public static class BigKey extends Key<String> {

        private String type;

        private DataSize memoryUsage;

        private long writeThroughput;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public DataSize getMemoryUsage() {
            return memoryUsage;
        }

        public void setMemoryUsage(DataSize memoryUsage) {
            this.memoryUsage = memoryUsage;
        }

        public long getWriteThroughput() {
            return writeThroughput;
        }

        public void setWriteThroughput(long throughput) {
            this.writeThroughput = throughput;
        }

        public DataSize writeBandwidth() {
            return DataSize.ofBytes(writeThroughput * memoryUsage.toBytes());
        }

    }

    private static class InternalBigKey extends Key<String> {

        private String type;

        private DataSize memoryUsage;

        private final Throughput writeThroughput = new Throughput();

        public synchronized long throughput() {
            return writeThroughput.calculateThroughput();
        }

        public void incrementWrites() {
            writeThroughput.add(1);
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public DataSize getMemoryUsage() {
            return memoryUsage;
        }

        public void setMemoryUsage(DataSize size) {
            this.memoryUsage = size;
        }

    }

}
