package com.redis.riot;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import net.spy.memcached.AddrUtil;

public class InetSocketAddressList {

	private List<InetSocketAddress> addresses;

	public InetSocketAddressList() {
	}

	public InetSocketAddressList(InetSocketAddress... addresses) {
		this(Arrays.asList(addresses));
	}

	public InetSocketAddressList(List<InetSocketAddress> addresses) {
		this.addresses = addresses;
	}

	public List<InetSocketAddress> getAddresses() {
		return addresses;
	}

	public void setAddresses(List<InetSocketAddress> addresses) {
		this.addresses = addresses;
	}

	public static InetSocketAddressList parse(String addresses) {
		return new InetSocketAddressList(AddrUtil.getAddresses(addresses));
	}
}
