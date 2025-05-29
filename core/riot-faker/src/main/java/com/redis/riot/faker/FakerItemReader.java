package com.redis.riot.faker;

import com.redis.spring.batch.item.AbstractCountingItemReader;
import net.datafaker.Faker;
import org.springframework.batch.item.ItemReader;
import org.springframework.util.Assert;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * {@link ItemReader} that generates HashMaps using Faker.
 *
 * @author Julien Ruaux
 */
public class FakerItemReader extends AbstractCountingItemReader<Map<String, Object>> {

	public static final Locale DEFAULT_LOCALE = Locale.getDefault();

	private Map<String, String> expressions = new LinkedHashMap<>();
	private Locale locale = DEFAULT_LOCALE;

	private final Object lock = new Object();

	private Faker faker;
	private Map<String, String> fields;

	public void setLocale(Locale locale) {
		this.locale = locale;
	}

	public void setExpressions(Map<String, String> fields) {
		this.expressions = fields;
	}

	@Override
	protected synchronized void doOpen() {
		Assert.notEmpty(expressions, "No field specified");
		if (fields == null) {
			fields = expressions.entrySet().stream().map(this::normalizeField)
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
		}
		faker = new Faker(locale);
	}

	private Entry<String, String> normalizeField(Entry<String, String> field) {
		if (field.getValue().startsWith("#{")) {
			return field;
		}
		return new AbstractMap.SimpleEntry<>(field.getKey(), "#{" + field.getValue() + "}");
	}

	@Override
	protected Map<String, Object> doRead() {
		Map<String, Object> map = new HashMap<>();
		for (Entry<String, String> field : fields.entrySet()) {
			String value;
			synchronized (lock) {
				value = faker.expression(field.getValue());
			}
			map.put(field.getKey(), value);
		}
		return map;
	}

	@Override
	protected synchronized void doClose() {
		faker = null;
	}

}
