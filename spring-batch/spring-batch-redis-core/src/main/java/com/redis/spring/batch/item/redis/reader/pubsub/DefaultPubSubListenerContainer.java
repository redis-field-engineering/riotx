package com.redis.spring.batch.item.redis.reader.pubsub;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.redis.batch.BatchUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class DefaultPubSubListenerContainer<K, V> implements PubSubListenerContainer<K, V> {

    private final Log log = LogFactory.getLog(getClass());

    private final Object lifecycleMonitor = new Object();

    private final AbstractRedisClient client;

    private final RedisCodec<K, V> codec;

    private boolean running = false;

    private final List<AbstractSubscription> subscriptions = new ArrayList<>();

    private StatefulRedisPubSubConnection<K, V> connection;

    public DefaultPubSubListenerContainer(AbstractRedisClient client, RedisCodec<K, V> codec) {
        this.client = client;
        this.codec = codec;
    }

    @Override
    public void start() {
        synchronized (lifecycleMonitor) {
            if (this.running) {
                return;
            }
            connection = connectPubSub();
            subscriptions.stream().filter(s -> !s.isActive()).forEach(AbstractSubscription::start);
            running = true;
        }
    }

    private StatefulRedisPubSubConnection<K, V> connectPubSub() {
        if (client instanceof RedisClusterClient) {
            log.info("Establishing Redis Cluster pub/sub connection");
            StatefulRedisClusterPubSubConnection<K, V> connection = ((RedisClusterClient) client).connectPubSub(codec);
            log.info("Enabling node message propagation");
            connection.setNodeMessagePropagation(true);
            return connection;
        }
        log.info("Establishing Redis pub/sub connection");
        return ((RedisClient) client).connectPubSub(codec);
    }

    @Override
    public void stop() {
        synchronized (lifecycleMonitor) {
            if (this.running) {
                subscriptions.forEach(Subscription::cancel);
                connection.close();
                running = false;
            }
        }
    }

    @Override
    public boolean isRunning() {
        synchronized (this.lifecycleMonitor) {
            return running;
        }
    }

    @Override
    public synchronized Subscription receive(K pattern, PubSubMessageListener<K, V> messageListener) {
        AbstractSubscription subscription = subscription(pattern, messageListener);
        synchronized (lifecycleMonitor) {
            subscriptions.add(subscription);
            if (this.running) {
                subscription.start();
            }
        }
        return subscription;
    }

    private AbstractSubscription subscription(K pattern, PubSubMessageListener<K, V> messageListener) {
        if (client instanceof RedisClusterClient) {
            return new ClusterSubscription(pattern, clusterPubSubListener(messageListener));
        }
        return new RedisSubscription(pattern, pubSubListener(messageListener));

    }

    private RedisPubSubListener<K, V> pubSubListener(PubSubMessageListener<K, V> listener) {
        return new RedisPubSubAdapter<>() {

            @Override
            public void message(K pattern, K channel, V message) {
                listener.onMessage(PubSubMessage.of(channel, message));
            }

        };
    }

    private RedisClusterPubSubListener<K, V> clusterPubSubListener(PubSubMessageListener<K, V> listener) {
        return new RedisClusterPubSubAdapter<K, V>() {

            @Override
            public void message(RedisClusterNode node, K pattern, K channel, V message) {
                listener.onMessage(PubSubMessage.of(channel, message));
            }

        };
    }

    private class RedisSubscription extends AbstractSubscription {

        private final RedisPubSubListener<K, V> listener;

        public RedisSubscription(K pattern, RedisPubSubListener<K, V> listener) {
            super(pattern);
            this.listener = listener;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void doStart() {
            connection.addListener(listener);
            log.info(String.format("Subscribing to pattern %s", BatchUtils.toString(pattern)));
            connection.sync().psubscribe(pattern);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void cancel() {
            log.info(String.format("Unsubscribing from pattern %s", BatchUtils.toString(pattern)));
            connection.sync().punsubscribe(pattern);
            connection.removeListener(listener);

        }

    }

    private class ClusterSubscription extends AbstractSubscription {

        private final RedisClusterPubSubListener<K, V> listener;

        public ClusterSubscription(K pattern, RedisClusterPubSubListener<K, V> listener) {
            super(pattern);
            this.listener = listener;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void doStart() {
            ((StatefulRedisClusterPubSubConnection<K, V>) connection).addListener(listener);
            log.info(String.format("Subscribing to pattern %s", BatchUtils.toString(pattern)));
            ((StatefulRedisClusterPubSubConnection<K, V>) connection).sync().upstream().commands().psubscribe(pattern);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void cancel() {
            log.info(String.format("Unsubscribing from pattern %s", BatchUtils.toString(pattern)));
            ((StatefulRedisClusterPubSubConnection<K, V>) connection).sync().upstream().commands().punsubscribe(pattern);
            ((StatefulRedisClusterPubSubConnection<K, V>) connection).removeListener(listener);

        }

    }

    private abstract class AbstractSubscription implements Subscription {

        protected final K pattern;

        private boolean active;

        protected AbstractSubscription(K pattern) {
            this.pattern = pattern;
        }

        @Override
        public boolean isActive() {
            return active;
        }

        protected void start() {
            doStart();
            this.active = true;
        }

        protected abstract void doStart();

    }

}
