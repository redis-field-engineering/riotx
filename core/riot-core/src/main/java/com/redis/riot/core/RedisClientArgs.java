package com.redis.riot.core;

import java.io.File;
import java.time.Duration;

import io.lettuce.core.RedisURI;
import io.lettuce.core.protocol.ProtocolVersion;

public interface RedisClientArgs {

	String DEFAULT_HOST = RedisURIBuilder.DEFAULT_HOST;
	int DEFAULT_PORT = RedisURIBuilder.DEFAULT_PORT;
	Duration DEFAULT_TIMEOUT = RedisURI.DEFAULT_TIMEOUT_DURATION;
	int DEFAULT_DATABASE = 0;
	ProtocolVersion DEFAULT_PROTOCOL_VERSION = ProtocolVersion.RESP2;
	int DEFAULT_POOL_SIZE = 8;
	ReadFrom DEFAULT_READ_FROM = ReadFrom.UPSTREAM;

	default String getHost() {
		return DEFAULT_HOST;
	}

	default int getPort() {
		return DEFAULT_PORT;
	}

	default String getSocket() {
		return null;
	}

	default String getUsername() {
		return null;
	}

	default char[] getPassword() {
		return null;
	}

	default Duration getTimeout() {
		return DEFAULT_TIMEOUT;
	}

	default int getDatabase() {
		return DEFAULT_DATABASE;
	}

	default boolean isTls() {
		return false;
	}

	default boolean isInsecure() {
		return false;
	}

	default String getClientName() {
		return null;
	}

	default boolean isCluster() {
		return false;
	}

	default ProtocolVersion getProtocolVersion() {
		return DEFAULT_PROTOCOL_VERSION;
	}

	default int getPoolSize() {
		return DEFAULT_POOL_SIZE;
	}

	default ReadFrom getReadFrom() {
		return DEFAULT_READ_FROM;
	}

	default File getKeystore() {
		return null;
	}

	default char[] getKeystorePassword() {
		return null;
	}

	default File getTruststore() {
		return null;
	}

	default char[] getTruststorePassword() {
		return null;
	}

	default File getKeyCert() {
		return null;
	}

	default File getKey() {
		return null;
	}

	default char[] getKeyPassword() {
		return null;
	}

	default File getTrustedCerts() {
		return null;
	}

}
