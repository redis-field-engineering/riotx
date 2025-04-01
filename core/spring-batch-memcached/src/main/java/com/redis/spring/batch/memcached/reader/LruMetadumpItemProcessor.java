package com.redis.spring.batch.memcached.reader;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;

import com.redis.spring.batch.memcached.ByteArrayTranscoder;
import com.redis.spring.batch.memcached.MemcachedEntry;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

public class LruMetadumpItemProcessor
		implements ItemProcessor<Chunk<? extends LruMetadumpEntry>, Chunk<MemcachedEntry>>, ItemStream {

	private final Transcoder<byte[]> transcoder = new ByteArrayTranscoder();

	private final MemcachedClient client;

	public LruMetadumpItemProcessor(MemcachedClient client) {
		this.client = client;
	}

	@Override
	public void close() throws ItemStreamException {
		client.shutdown();
	}

	@Override
	public Chunk<MemcachedEntry> process(Chunk<? extends LruMetadumpEntry> items) {
		Iterator<String> keys = StreamSupport.stream(items.spliterator(), false).map(LruMetadumpEntry::getKey)
				.iterator();
		Map<String, byte[]> values = client.getBulk(keys, transcoder);
		Chunk<MemcachedEntry> entries = new Chunk<>();
		for (LruMetadumpEntry metaEntry : items) {
			MemcachedEntry entry = new MemcachedEntry();
			entry.setKey(metaEntry.getKey());
			entry.setValue(values.get(metaEntry.getKey()));
			entry.setExpiration(metaEntry.getExp());
			entries.add(entry);
		}
		return entries;
	}

}
