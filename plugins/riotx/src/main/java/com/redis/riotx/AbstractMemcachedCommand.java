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

import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.spring.batch.memcached.MemcachedException;
import com.redis.spring.batch.memcached.MemcachedWriteException;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import picocli.CommandLine.ArgGroup;

public abstract class AbstractMemcachedCommand extends AbstractJobCommand<Main> {

	@ArgGroup(exclusive = false)
	private MemcachedArgs memcachedArgs = new MemcachedArgs();

	protected Supplier<MemcachedClient> clientSupplier;

	@Override
	protected void setup() {
		super.setup();
		if (clientSupplier == null) {
			clientSupplier = this::memcachedClient;
		}
	}

	private MemcachedClient memcachedClient() {
		return memcachedClient(memcachedArgs.getAddresses(), memcachedArgs.isTls());
	}

	@Override
	protected void execute() throws Exception {
		super.execute();
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

	@Override
	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		FaultTolerantStepBuilder<I, O> ftStep = super.faultTolerant(step);
		ftStep.skip(MemcachedWriteException.class);
		ftStep.noRetry(MemcachedWriteException.class);
		ftStep.noSkip(TimeoutException.class);
		ftStep.retry(TimeoutException.class);
		return ftStep;
	}

	public MemcachedArgs getMemcachedArgs() {
		return memcachedArgs;
	}

	public void setMemcachedArgs(MemcachedArgs memcachedArgs) {
		this.memcachedArgs = memcachedArgs;
	}

}
