package com.redis.spring.batch.item.redis.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.Range;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class GeneratorItemReader extends AbstractItemCountingItemStreamItemReader<KeyValue<String>> {

	public static final String EVENT = "datagen";
	public static final String DEFAULT_KEYSPACE = "gen";
	public static final String DEFAULT_KEY_SEPARATOR = ":";
	public static final Range DEFAULT_KEY_RANGE = new Range(1, Range.UPPER_BORDER_NOT_DEFINED);

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'
	private static final Random random = new Random();
	private final ObjectMapper mapper = new ObjectMapper();

	public static List<ItemType> defaultTypes() {
		return Arrays.asList(ItemType.HASH, ItemType.JSON, ItemType.LIST, ItemType.SET, ItemType.STREAM,
				ItemType.STRING, ItemType.ZSET);
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
	private List<ItemType> types = defaultTypes();

	public GeneratorItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	private String key() {
		return key(index(keyRange));
	}

	private int index(Range range) {
		return range.getMin() + (getCurrentItemCount() - 1) % (range.getMax() - range.getMin());
	}

	public String key(int index) {
        return keyspace + keySeparator + index;
	}

	private Object value(String key, ItemType type) throws JsonProcessingException {
		switch (type) {
		case HASH:
			return hash();
		case LIST:
			return list();
		case SET:
			return set();
		case STREAM:
			return streamMessages(key);
		case STRING:
			return string();
		case ZSET:
			return zset();
		case JSON:
			return json();
		case TIMESERIES:
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
			long time = startTime + getCurrentItemCount() + index;
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
		return random.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();
	}

	private Stream<String> members(CollectionOptions options) {
		int spread = options.getMemberRange().getMax() - options.getMemberRange().getMin() + 1;
		return IntStream.range(0, randomInt(options.getMemberCount()))
				.map(i -> options.getMemberRange().getMin() + i % spread).mapToObj(String::valueOf);
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

	@Override
	protected void doOpen() {
		// do nothing
	}

	@Override
	protected void doClose() {
		// do nothing
	}

	@Override
	protected KeyValue<String> doRead() throws JsonProcessingException {
		String key = key();
		ItemType type = type();
		long ttl = ttl();
		Object value = value(key, type);
		KeyValue<String> keyValue = new KeyValue<>();
		keyValue.setKey(key);
		keyValue.setEvent(EVENT);
		keyValue.setType(type.getString());
		keyValue.setTtl(ttl);
		keyValue.setValue(value);
		return keyValue;
	}

	private ItemType type() {
		return types.get(getCurrentItemCount() % types.size());
	}

	private long ttl() {
		if (expiration == null) {
			return KeyValue.TTL_NONE;
		}
		return System.currentTimeMillis() + randomInt(expiration);
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

	public List<ItemType> getTypes() {
		return types;
	}

	public void setTypes(List<ItemType> types) {
		this.types = types;
	}

	public void setTypes(ItemType... types) {
		setTypes(Arrays.asList(types));
	}
}
