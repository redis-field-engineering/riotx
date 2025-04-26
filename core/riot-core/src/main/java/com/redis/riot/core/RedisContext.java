package com.redis.riot.core;

import org.springframework.beans.factory.InitializingBean;

import com.redis.lettucemod.RedisModulesClientBuilder;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.reader.RedisLiveItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.resource.ClientResources;
import lombok.ToString;

@ToString
public class RedisContext implements InitializingBean, AutoCloseable {

    private RedisURI uri;

    private boolean cluster;

    private ProtocolVersion protocolVersion;

    private SslOptions sslOptions = ClientOptions.DEFAULT_SSL_OPTIONS;

    private int poolSize = RedisItemReader.DEFAULT_POOL_SIZE;

    private ClientResources clientResources;

    private ReadFrom readFrom;

    private AbstractRedisClient client;

    private StatefulRedisModulesConnection<String, String> connection;

    @Override
    public void afterPropertiesSet() {
        RedisModulesClientBuilder clientBuilder = new RedisModulesClientBuilder();
        clientBuilder.cluster(cluster);
        clientBuilder.options(clientOptions());
        clientBuilder.uri(uri);
        clientBuilder.resources(clientResources);
        this.client = clientBuilder.build();
        this.connection = BatchUtils.connection(client);
    }

    private ClientOptions clientOptions() {
        ClientOptions.Builder options = cluster ? ClusterClientOptions.builder() : ClientOptions.builder();
        options.protocolVersion(protocolVersion);
        options.sslOptions(sslOptions);
        return options.build();
    }

    public <K, V> void configure(RedisItemReader<K, V> reader) {
        reader.setClient(client);
        reader.setPoolSize(poolSize);
        if (reader instanceof RedisLiveItemReader) {
            ((RedisLiveItemReader<K, V>) reader).setDatabase(uri.getDatabase());
        }
        reader.setReadFrom(readFrom);
    }

    public <K, V, T> void configure(RedisItemWriter<K, V, T> writer) {
        writer.setClient(client);
        writer.setPoolSize(poolSize);
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
            client.getResources().shutdown();
        }
    }

    public AbstractRedisClient getClient() {
        return client;
    }

    public StatefulRedisModulesConnection<String, String> getConnection() {
        return connection;
    }

    public RedisURI getUri() {
        return uri;
    }

    public RedisContext uri(RedisURI uri) {
        this.uri = uri;
        return this;
    }

    public boolean isCluster() {
        return cluster;
    }

    public RedisContext cluster(boolean cluster) {
        this.cluster = cluster;
        return this;
    }

    public ProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    public RedisContext protocolVersion(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;
        return this;
    }

    public SslOptions getSslOptions() {
        return sslOptions;
    }

    public RedisContext sslOptions(SslOptions sslOptions) {
        this.sslOptions = sslOptions;
        return this;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public RedisContext poolSize(int size) {
        this.poolSize = size;
        return this;
    }

    public ReadFrom getReadFrom() {
        return readFrom;
    }

    public RedisContext readFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
        return this;
    }

    public ClientResources getClientResources() {
        return clientResources;
    }

    public RedisContext clientResources(ClientResources clientResources) {
        this.clientResources = clientResources;
        return this;
    }

}
