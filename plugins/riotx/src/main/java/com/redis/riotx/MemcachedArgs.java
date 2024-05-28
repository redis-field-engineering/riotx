package com.redis.riotx;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import picocli.CommandLine.Option;

public class MemcachedArgs {

	private static final InetSocketAddress DEFAULT_ADDRESS = new InetSocketAddress("localhost", 11211);

	@Option(names = "--address", description = "Server address (default: ${DEFAULT-VALUE}).", paramLabel = "<add>")
	private List<InetSocketAddress> addresses = defaultAddresses();

	@Option(names = "--tls", description = "Establish a secure TLS connection.")
	private boolean tls;

	public static List<InetSocketAddress> defaultAddresses() {
		return Arrays.asList(DEFAULT_ADDRESS);
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
