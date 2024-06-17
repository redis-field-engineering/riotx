package com.redis.riotx;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.Step;
import com.redis.spring.batch.memcached.MemcachedException;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;

public abstract class AbstractMemcachedCommand extends AbstractJobCommand {

	protected Supplier<MemcachedClient> clientSupplier;

	@Override
	public void afterPropertiesSet() throws Exception {
		System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SLF4JLogger");
		super.afterPropertiesSet();
		if (clientSupplier == null) {
			clientSupplier = this::memcachedClient;
		}
	}

	protected <I, O> Step<I, O> memcachedStep(String name, ItemReader<I> reader, ItemWriter<O> writer) {
		Step<I, O> step = new Step<>(name, reader, writer);
		step.skip(MemcachedException.class);
		step.noRetry(MemcachedException.class);
		step.noSkip(TimeoutException.class);
		step.retry(TimeoutException.class);
		return step;
	}

	@Override
	protected void shutdown() {
		clientSupplier = null;
	}

	protected MemcachedClient memcachedClient(List<InetSocketAddress> addresses, boolean tls) {
		try {
			if (tls) {
				return new MemcachedClient(sslConnectionFactory(), addresses);
			}
			return new MemcachedClient(addresses);
		} catch (IOException e) {
			throw new MemcachedException("Could not create Memcached client", e);
		}
	}

	private ConnectionFactory sslConnectionFactory() {
		try {
			TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			tmf.init((KeyStore) null);
			SSLContext sslContext = SSLContext.getInstance("TLS");
			sslContext.init(null, tmf.getTrustManagers(), null);
			return new ConnectionFactoryBuilder().setSSLContext(sslContext).build();
		} catch (GeneralSecurityException e) {
			throw new MemcachedException("Could not create SSL connection factory", e);
		}
	}

	protected abstract MemcachedClient memcachedClient();

}
