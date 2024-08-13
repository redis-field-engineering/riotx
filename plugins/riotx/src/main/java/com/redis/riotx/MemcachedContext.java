package com.redis.riotx;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.redis.riot.core.RiotException;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;

public class MemcachedContext implements InitializingBean, AutoCloseable {

	private List<InetSocketAddress> addresses;
	private boolean tls;

	private ConnectionFactory sslConnectionFactory;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notEmpty(addresses, "No Memcached server address specified");
		if (tls) {
			sslConnectionFactory = sslConnectionFactory();
		}
	}

	@Override
	public void close() {
		sslConnectionFactory = null;
	}

	public MemcachedClient memcachedClient() {
		try {
			if (tls) {
				return new MemcachedClient(sslConnectionFactory, addresses);
			}
			return new MemcachedClient(addresses);
		} catch (IOException e) {
			throw new RiotException("Could not create Memcached client", e);
		}
	}

	private ConnectionFactory sslConnectionFactory() throws GeneralSecurityException {
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init((KeyStore) null);
		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(null, tmf.getTrustManagers(), null);
		return new ConnectionFactoryBuilder().setSSLContext(sslContext).build();
	}

	public List<InetSocketAddress> getAddresses() {
		return addresses;
	}

	public void setAddresses(List<InetSocketAddress> addresses) {
		this.addresses = addresses;
	}

	public boolean isTls() {
		return tls;
	}

	public void setTls(boolean tls) {
		this.tls = tls;
	}

}
