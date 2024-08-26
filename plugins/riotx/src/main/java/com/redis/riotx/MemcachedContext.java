package com.redis.riotx;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;

public class MemcachedContext {

	private final List<InetSocketAddress> addresses;
	private boolean tls;

	private String hostnameForTlsVerification;

	private boolean skipTlsHostnameVerification;
	private ConnectionFactory connectionFactory;

	public MemcachedContext(List<InetSocketAddress> addresses) {
		this.addresses = addresses;
	}

	public MemcachedClient memcachedClient() throws IOException {
		if (tls) {
			return new MemcachedClient(connectionFactory, addresses);
		}
		return new MemcachedClient(addresses);
	}

	public MemcachedClient safeMemcachedClient() {
		try {
			return memcachedClient();
		} catch (IOException e) {
			throw new RuntimeException("Could not create memcached client", e);
		}
	}

	private ConnectionFactory sslConnectionFactory() throws GeneralSecurityException {
		ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
		// Build SSLContext
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init((KeyStore) null);
		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(null, tmf.getTrustManagers(), null);
		builder.setSSLContext(sslContext);
		// TLS mode enables hostname verification by default. It is always recommended
		// to do that.
		builder.setHostnameForTlsVerification(hostnameForTlsVerification);
		if (skipTlsHostnameVerification) {
			builder.setSkipTlsHostnameVerification(true);
		}
		// To disable hostname verification, do the following:
		return builder.setSSLContext(sslContext).build();
	}

	public boolean isTls() {
		return tls;
	}

	public void setTls(boolean tls) throws GeneralSecurityException {
		if (tls) {
			connectionFactory = sslConnectionFactory();
		}
		this.tls = tls;
	}

	public String getHostnameForTlsVerification() {
		return hostnameForTlsVerification;
	}

	public void setHostnameForTlsVerification(String hostname) {
		this.hostnameForTlsVerification = hostname;
	}

	public boolean isSkipTlsHostnameVerification() {
		return skipTlsHostnameVerification;
	}

	public void setSkipTlsHostnameVerification(boolean skipTlsHostnameVerification) {
		this.skipTlsHostnameVerification = skipTlsHostnameVerification;
	}

}
