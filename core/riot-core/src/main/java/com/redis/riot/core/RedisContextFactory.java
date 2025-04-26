package com.redis.riot.core;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslVerifyMode;

public class RedisContextFactory {


    private static RedisURIBuilder uriBuilder(RedisClientArgs args) {
        RedisURIBuilder builder = new RedisURIBuilder();
        builder.clientName(args.getClientName());
        builder.database(args.getDatabase());
        builder.host(args.getHost());
        builder.password(args.getPassword());
        builder.port(args.getPort());
        builder.timeout(args.getTimeout());
        builder.socket(args.getSocket());
        builder.tls(args.isTls());
        builder.username(args.getUsername());
        if (args.isInsecure()) {
            builder.verifyMode(SslVerifyMode.NONE);
        }
        return builder;
    }

    public static RedisContext create(RedisURI uri, RedisClientArgs args) {
        RedisContext context = new RedisContext();
        context.cluster(args.isCluster());
        context.poolSize(args.getPoolSize());
        context.protocolVersion(args.getProtocolVersion());
        context.readFrom(args.getReadFrom().getReadFrom());
        context.uri(uriBuilder(args).uri(uri).build());
        context.sslOptions(sslOptions(args));
        return context;
    }

    private static SslOptions sslOptions(RedisClientArgs args) {
        SslOptions.Builder ssl = SslOptions.builder();
        if (args.getKey() != null) {
            ssl.keyManager(args.getKeyCert(), args.getKey(), args.getKeyPassword());
        }
        if (args.getKeystore() != null) {
            ssl.keystore(args.getKeystore(), args.getKeystorePassword());
        }
        if (args.getTruststore() != null) {
            ssl.truststore(SslOptions.Resource.from(args.getTruststore()), args.getTruststorePassword());
        }
        if (args.getTrustedCerts() != null) {
            ssl.trustManager(args.getTrustedCerts());
        }
        return ssl.build();
    }

}
