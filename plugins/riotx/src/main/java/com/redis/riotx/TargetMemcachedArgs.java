package com.redis.riotx;

import java.net.InetSocketAddress;
import java.util.List;

import picocli.CommandLine.Option;

public class TargetMemcachedArgs {

	@Option(names = "--target-address", description = "Target server address (default: ${DEFAULT-VALUE}).", paramLabel = "<add>")
	private List<InetSocketAddress> addresses = MemcachedArgs.defaultAddresses();

	@Option(names = "--target-tls", description = "Establish a secure TLS connection to target server.")
	private boolean tls;

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
