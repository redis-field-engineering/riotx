package com.redis.spring.batch.memcached.test;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class MemcachedContainer extends GenericContainer<MemcachedContainer> {

	public static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("memcached");

	public static final String DEFAULT_TAG = "1.6-alpine";

	public MemcachedContainer(String dockerImageName) {
		this(DockerImageName.parse(dockerImageName));
	}

	public MemcachedContainer(final DockerImageName dockerImageName) {
		super(dockerImageName);
		withExposedPorts(11211);
	}

	public String getAddress() {
		return getHost() + ":" + getFirstMappedPort();
	}

}