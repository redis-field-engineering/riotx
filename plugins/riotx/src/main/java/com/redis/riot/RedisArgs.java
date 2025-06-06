package com.redis.riot;

import com.redis.lettucemod.utils.SslOptionsBuilder;
import com.redis.lettucemod.utils.URIBuilder;
import com.redis.riot.core.ReadFrom;
import com.redis.riot.core.RedisContext;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.protocol.ProtocolVersion;

import java.io.File;
import java.time.Duration;

public interface RedisArgs {

    ProtocolVersion DEFAULT_PROTOCOL_VERSION = ProtocolVersion.RESP2;

    int DEFAULT_POOL_SIZE = RedisContext.DEFAULT_POOL_SIZE;

    ReadFrom DEFAULT_READ_FROM = ReadFrom.UPSTREAM;

    String DEFAULT_HOST = URIBuilder.DEFAULT_HOST;

    int DEFAULT_PORT = URIBuilder.DEFAULT_PORT;

    int DEFAULT_DATABASE = 0;

    Duration DEFAULT_TIMEOUT = URIBuilder.DEFAULT_TIMEOUT_DURATION;

    SslVerifyMode DEFAULT_SSL_VERIFY_MODE = URIBuilder.DEFAULT_VERIFY_MODE;

    default RedisURI redisURI(RedisURI uri) {
        URIBuilder builder = new URIBuilder();
        builder.clientName(getClientName());
        builder.database(getDatabase());
        builder.host(getHost());
        builder.password(getPassword());
        builder.port(getPort());
        builder.socket(getSocket());
        builder.timeout(getTimeout());
        builder.tls(isTls());
        builder.uri(uri);
        builder.username(getUsername());
        builder.verifyMode(isInsecure() ? SslVerifyMode.NONE : getSslVerifyMode());
        return builder.build();
    }

    default ReadFrom getReadFrom() {
        return DEFAULT_READ_FROM;
    }

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

    default boolean isInsecure() {
        return false;
    }

    default Duration getTimeout() {
        return DEFAULT_TIMEOUT;
    }

    default boolean isTls() {
        return false;
    }

    default SslVerifyMode getSslVerifyMode() {
        return DEFAULT_SSL_VERIFY_MODE;
    }

    default String getClientName() {
        return null;
    }

    default int getDatabase() {
        return DEFAULT_DATABASE;
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

    default SslOptions sslOptions() {
        SslOptionsBuilder builder = new SslOptionsBuilder();
        builder.key(getKey());
        builder.keyCert(getKeyCert());
        builder.keyPassword(getKeyPassword());
        builder.keystore(getKeystore());
        builder.keystorePassword(getKeystorePassword());
        builder.trustedCerts(getTrustedCerts());
        builder.truststore(getTruststore());
        builder.truststorePassword(getTruststorePassword());
        return builder.build();
    }

    default RedisContext redisContext(RedisURI uri) {
        RedisContext context = new RedisContext();
        context.cluster(isCluster());
        context.poolSize(getPoolSize());
        context.protocolVersion(getProtocolVersion());
        context.readFrom(getReadFrom().getReadFrom());
        context.sslOptions(sslOptions());
        context.uri(redisURI(uri));
        return context;
    }

}
