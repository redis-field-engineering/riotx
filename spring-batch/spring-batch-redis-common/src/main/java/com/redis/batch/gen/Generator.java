package com.redis.batch.gen;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.batch.*;
import com.redis.lettucemod.timeseries.Sample;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Generator {

    public static final String EVENT = "datagen";

    public static final String DEFAULT_KEYSPACE = "gen";

    public static final String DEFAULT_KEY_SEPARATOR = ":";

    public static final Range DEFAULT_KEY_RANGE = new Range(1, Range.UPPER_BORDER_NOT_DEFINED);

    private static final int LEFT_LIMIT = 48; // numeral '0'

    private static final int RIGHT_LIMIT = 122; // letter 'z'

    private static final Random random = new Random();

    private final ObjectMapper mapper = new ObjectMapper();

    public static List<KeyType> defaultTypes() {
        return Arrays.asList(KeyType.hash, KeyType.json, KeyType.list, KeyType.set, KeyType.stream, KeyType.string,
                KeyType.zset);
    }

    private String keySeparator = DEFAULT_KEY_SEPARATOR;

    private String keyspace = DEFAULT_KEYSPACE;

    private Range keyRange = DEFAULT_KEY_RANGE;

    private Range expiration;

    private MapOptions hashOptions = new MapOptions();

    private StreamOptions streamOptions = new StreamOptions();

    private TimeSeriesOptions timeSeriesOptions = new TimeSeriesOptions();

    private MapOptions jsonOptions = new MapOptions();

    private CollectionOptions listOptions = new CollectionOptions();

    private CollectionOptions setOptions = new CollectionOptions();

    private StringOptions stringOptions = new StringOptions();

    private ZsetOptions zsetOptions = new ZsetOptions();

    private List<KeyType> types = defaultTypes();

    private final AtomicLong currentIndex = new AtomicLong();

    private String key() {
        return key(index(keyRange));
    }

    private long index(Range range) {
        return range.getMin() + currentIndex.get() % (range.getMax() - range.getMin());
    }

    public String key(long index) {
        return keyspace + keySeparator + index;
    }

    private Object value(String key, KeyType type) throws JsonProcessingException {
        switch (type) {
            case hash:
                return hash();
            case list:
                return list();
            case set:
                return set();
            case stream:
                return streamMessages(key);
            case string:
                return string();
            case zset:
                return zset();
            case json:
                return json();
            case timeseries:
                return samples();
            default:
                return null;
        }
    }

    private String json() throws JsonProcessingException {
        return mapper.writeValueAsString(map(jsonOptions));
    }

    private Map<String, String> hash() {
        return map(hashOptions);
    }

    private String string() {
        return string(stringOptions.getLength());
    }

    private List<String> list() {
        return members(listOptions).collect(Collectors.toList());
    }

    private Set<String> set() {
        return members(setOptions).collect(Collectors.toSet());
    }

    private List<Sample> samples() {
        List<Sample> samples = new ArrayList<>();
        int size = randomInt(timeSeriesOptions.getSampleCount());
        long startTime = timeSeriesStartTime();
        for (int index = 0; index < size; index++) {
            long time = startTime + currentIndex.get() + index;
            samples.add(Sample.of(time, random.nextDouble()));
        }
        return samples;
    }

    private long timeSeriesStartTime() {
        return timeSeriesOptions.getStartTime().toEpochMilli();
    }

    private Set<ScoredValue<String>> zset() {
        return members(zsetOptions).map(this::scoredValue).collect(Collectors.toSet());
    }

    private ScoredValue<String> scoredValue(String value) {
        double score = randomDouble(zsetOptions.getScore());
        return ScoredValue.just(score, value);
    }

    private Collection<StreamMessage<String, String>> streamMessages(String key) {
        Collection<StreamMessage<String, String>> messages = new ArrayList<>();
        for (int elementIndex = 0; elementIndex < randomInt(streamOptions.getMessageCount()); elementIndex++) {
            messages.add(new StreamMessage<>(key, null, map(streamOptions.getBodyOptions())));
        }
        return messages;
    }

    private Map<String, String> map(MapOptions options) {
        Map<String, String> hash = new HashMap<>();
        for (int index = 0; index < randomInt(options.getFieldCount()); index++) {
            int fieldIndex = index + 1;
            hash.put("field" + fieldIndex, string(options.getFieldLength()));
        }
        return hash;
    }

    private String string(Range range) {
        int length = randomInt(range);
        return string(length);
    }

    public static String string(int length) {
        return random.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97)).limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    }

    private Stream<String> members(CollectionOptions options) {
        int spread = options.getMemberRange().getMax() - options.getMemberRange().getMin() + 1;
        return IntStream.range(0, randomInt(options.getMemberCount())).map(i -> options.getMemberRange().getMin() + i % spread)
                .mapToObj(String::valueOf);
    }

    private int randomInt(Range range) {
        if (range.getMin() == range.getMax()) {
            return range.getMin();
        }
        return ThreadLocalRandom.current().nextInt(range.getMin(), range.getMax());
    }

    private double randomDouble(Range range) {
        if (range.getMin() == range.getMax()) {
            return range.getMin();
        }
        return ThreadLocalRandom.current().nextDouble(range.getMin(), range.getMax());
    }

    public KeyStructEvent<String, String> next() throws JsonProcessingException {
        String key = key();
        KeyStructEvent<String, String> keyValueEvent = new KeyStructEvent<>();
        keyValueEvent.setKey(key);
        keyValueEvent.setEvent(EVENT);
        keyValueEvent.setOperation(KeyOperation.CREATE);
        keyValueEvent.setTimestamp(Instant.now());
        keyValueEvent.setType(type());
        keyValueEvent.setTtl(ttl());
        keyValueEvent.setValue(value(key, keyValueEvent.getType()));
        currentIndex.incrementAndGet();
        return keyValueEvent;
    }

    private KeyType type() {
        return types.get((int) currentIndex.get() % types.size());
    }

    private Instant ttl() {
        if (expiration == null) {
            return null;
        }
        return Instant.now().plusMillis(randomInt(expiration));
    }

    public String getKeySeparator() {
        return keySeparator;
    }

    public void setKeySeparator(String keySeparator) {
        this.keySeparator = keySeparator;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public Range getKeyRange() {
        return keyRange;
    }

    public void setKeyRange(Range keyRange) {
        this.keyRange = keyRange;
    }

    public Range getExpiration() {
        return expiration;
    }

    public void setExpiration(Range expiration) {
        this.expiration = expiration;
    }

    public MapOptions getHashOptions() {
        return hashOptions;
    }

    public void setHashOptions(MapOptions hashOptions) {
        this.hashOptions = hashOptions;
    }

    public StreamOptions getStreamOptions() {
        return streamOptions;
    }

    public void setStreamOptions(StreamOptions streamOptions) {
        this.streamOptions = streamOptions;
    }

    public TimeSeriesOptions getTimeSeriesOptions() {
        return timeSeriesOptions;
    }

    public void setTimeSeriesOptions(TimeSeriesOptions timeSeriesOptions) {
        this.timeSeriesOptions = timeSeriesOptions;
    }

    public MapOptions getJsonOptions() {
        return jsonOptions;
    }

    public void setJsonOptions(MapOptions jsonOptions) {
        this.jsonOptions = jsonOptions;
    }

    public CollectionOptions getListOptions() {
        return listOptions;
    }

    public void setListOptions(CollectionOptions listOptions) {
        this.listOptions = listOptions;
    }

    public CollectionOptions getSetOptions() {
        return setOptions;
    }

    public void setSetOptions(CollectionOptions setOptions) {
        this.setOptions = setOptions;
    }

    public StringOptions getStringOptions() {
        return stringOptions;
    }

    public void setStringOptions(StringOptions stringOptions) {
        this.stringOptions = stringOptions;
    }

    public ZsetOptions getZsetOptions() {
        return zsetOptions;
    }

    public void setZsetOptions(ZsetOptions zsetOptions) {
        this.zsetOptions = zsetOptions;
    }

    public List<KeyType> getTypes() {
        return types;
    }

    public void setTypes(List<KeyType> types) {
        this.types = types;
    }

    public void setTypes(KeyType... types) {
        setTypes(Arrays.asList(types));
    }

    public void setCurrentIndex(long index) {
        this.currentIndex.set(index);
    }

}
