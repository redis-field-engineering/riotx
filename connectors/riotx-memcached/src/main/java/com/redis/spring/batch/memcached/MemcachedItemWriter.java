package com.redis.spring.batch.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import com.redis.spring.batch.memcached.impl.ByteArrayTranscoder;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

public class MemcachedItemWriter implements ItemStreamWriter<MemcachedEntry> {

	private static final Transcoder<byte[]> transcoder = new ByteArrayTranscoder();

	private MemcachedClient client;
	private final List<InetSocketAddress> addresses;

	public MemcachedItemWriter(InetSocketAddress... addresses) {
		this(Arrays.asList(addresses));
	}

	public MemcachedItemWriter(List<InetSocketAddress> addresses) {
		this.addresses = addresses;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (client == null) {
			try {
				client = new MemcachedClient(addresses);
			} catch (IOException e) {
				throw new ItemStreamException("Could not initialize client", e);
			}
		}
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		if (client != null) {
			client.shutdown();
			client = null;
		}
	}

	@Override
	public void write(Chunk<? extends MemcachedEntry> chunk) throws Exception {
		for (MemcachedEntry entry : chunk) {
			client.set(entry.getKey(), entry.getExpiration(), entry.getValue(), transcoder);
		}
	}
}
