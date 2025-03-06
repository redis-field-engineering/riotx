package com.redis.riotx.stats;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.redis.spring.batch.item.redis.common.KeyValue;

public class DatabaseAnalyzer {

	private static final Pattern keyspacePattern = Pattern.compile("^([^:]+):");

	private final Map<String, KeyspaceStats> stats = new HashMap<>();

	public void add(KeyValue<String> kv) {
		String keyspace = extractKeyspace(kv.getKey());
		if (keyspace == null) {
			return;
		}
		stats.computeIfAbsent(keyspace, KeyspaceStats::new).add(kv);
	}

	private String extractKeyspace(String key) {
		var matcher = keyspacePattern.matcher(key);
		return matcher.find() ? matcher.group(1) : null;
	}

	public Collection<KeyspaceStats> keyspaceStats() {
		return stats.values();
	}

}