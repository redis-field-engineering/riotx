package com.redis.riot.core;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.utils.ClientBuilder;
import com.redis.lettucemod.utils.ConnectionBuilder;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.reader.RedisLiveItemReader;
import io.lettuce.core.*;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.resource.ClientResources;
import lombok.ToString;
import org.springframework.beans.factory.InitializingBean;

@ToString
public class RedisContext implements InitializingBean, AutoCloseable {

    public static int DEFAULT_POOL_SIZE = 8;

    private RedisURI uri;

    private boolean cluster;

    private ProtocolVersion protocolVersion;

    private SslOptions sslOptions;

    private int poolSize = DEFAULT_POOL_SIZE;

    private ClientResources clientResources;

    private ReadFrom readFrom;

    private AbstractRedisClient client;

    private StatefulRedisModulesConnection<String, String> connection;

    @Override
    public void afterPropertiesSet() {
        ClientBuilder clientBuilder = ClientBuilder.of(uri);
        clientBuilder.cluster(cluster);
        clientBuilder.options(clientOptions());
        clientBuilder.resources(clientResources);
        this.client = clientBuilder.build();
        this.connection = ConnectionBuilder.client(client).readFrom(readFrom).connection();
    }

    private ClientOptions clientOptions() {
        ClientOptions.Builder options = cluster ? ClusterClientOptions.builder() : ClientOptions.builder();
        if (protocolVersion != null) {
            options.protocolVersion(protocolVersion);
        }
        if (sslOptions != null) {
            options.sslOptions(sslOptions);
        }
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

    public AbstractRedisClient client() {
        return client;
    }

    public StatefulRedisModulesConnection<String, String> getConnection() {
        return connection;
    }

    public RedisURI uri() {
        return uri;
    }

    public RedisContext uri(RedisURI uri) {
        this.uri = uri;
        return this;
    }

    public boolean cluster() {
        return cluster;
    }

    public RedisContext cluster(boolean cluster) {
        this.cluster = cluster;
        return this;
    }

    public ProtocolVersion protocolVersion() {
        return protocolVersion;
    }

    public RedisContext protocolVersion(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;
        return this;
    }

    public SslOptions sslOptions() {
        return sslOptions;
    }

    public RedisContext sslOptions(SslOptions sslOptions) {
        this.sslOptions = sslOptions;
        return this;
    }

    public int poolSize() {
        return poolSize;
    }

    public RedisContext poolSize(int size) {
        this.poolSize = size;
        return this;
    }

    public ReadFrom readFrom() {
        return readFrom;
    }

    public RedisContext readFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
        return this;
    }

    public ClientResources clientResources() {
        return clientResources;
    }

    public RedisContext clientResources(ClientResources clientResources) {
        this.clientResources = clientResources;
        return this;
    }

}
