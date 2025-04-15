package com.redis.spring.batch.item.redis.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;

public class KeyComparisonStatsWriter<K> implements ItemWriter<KeyComparison<K>> {

    private final Map<Status, Map<String, AtomicLong>> counts = new HashMap<>();

    @Override
    public void write(Chunk<? extends KeyComparison<K>> chunk) throws Exception {
        chunk.forEach(this::add);
    }

    public long add(KeyComparison<?> comparison) {
        Map<String, AtomicLong> typeCounts = counts.computeIfAbsent(comparison.getStatus(), s -> new HashMap<>());
        AtomicLong count = typeCounts.computeIfAbsent(comparison.getSource().getType(), t -> new AtomicLong());
        return count.incrementAndGet();
    }

    public List<KeyComparisonStat> allStats() {
        List<KeyComparisonStat> stats = new ArrayList<>();
        counts.forEach((status, map) -> {
            map.forEach((type, count) -> {
                KeyComparisonStat stat = new KeyComparisonStat();
                stat.setStatus(status);
                stat.setType(type);
                stat.setCount(count.get());
                stats.add(stat);
            });
        });
        return stats;
    }

}
