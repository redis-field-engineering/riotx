package com.redis.riotx.stats;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.redis.spring.batch.item.redis.common.KeyValue;

/**
 * Aggregates statistics at the keyspace level
 */
public class KeyspaceStats {

	private final String keyspace;
	private final Map<String, KeyTypeStats> stats = new HashMap<>();

	public KeyspaceStats(String keyspace) {
		this.keyspace = keyspace;
	}

	public void add(KeyValue<String> item) {
		stats.computeIfAbsent(item.getType(), KeyTypeStats::new).add(item);
	}

	public String getKeyspace() {
		return keyspace;
	}

	public Collection<KeyTypeStats> typeStats() {
		return stats.values();
	}

}
