package com.redis.spring.batch.item.redis.reader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;

public class KeyComparisonStats {

	private static class StatKey {

		private Status status;
		private String type;

		public Status getStatus() {
			return status;
		}

		public void setStatus(Status status) {
			this.status = status;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		@Override
		public int hashCode() {
			return Objects.hash(status, type);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StatKey other = (StatKey) obj;
			return status == other.status && Objects.equals(type, other.type);
		}

		public static StatKey of(Status status, String type) {
			StatKey key = new StatKey();
			key.setStatus(status);
			key.setType(type);
			return key;
		}

	}

	private final Map<StatKey, AtomicLong> counts = new HashMap<>();

	public void add(KeyComparison<?> comparison) {
		getCount(comparison).incrementAndGet();
	}

	private AtomicLong getCount(KeyComparison<?> comparison) {
		StatKey key = StatKey.of(comparison.getStatus(), comparison.getSource().getType());
		return counts.computeIfAbsent(key, k -> new AtomicLong());
	}

	public List<KeyComparisonStat> allStats() {
		return counts.entrySet().stream().map(KeyComparisonStats::stat).collect(Collectors.toList());
	}

	private static KeyComparisonStat stat(Entry<StatKey, AtomicLong> entry) {
		return KeyComparisonStat.of(entry.getKey().getStatus(), entry.getKey().getType(), entry.getValue().get());
	}

}
