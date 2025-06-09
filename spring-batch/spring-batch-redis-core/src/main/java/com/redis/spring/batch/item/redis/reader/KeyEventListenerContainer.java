package com.redis.spring.batch.item.redis.reader;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.redis.batch.KeyEvent;
import com.redis.lettucemod.utils.ConnectionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.batch.BatchUtils;
import com.redis.batch.KeyValue;
import com.redis.spring.batch.item.redis.reader.pubsub.PubSubListenerContainer;
import com.redis.spring.batch.item.redis.reader.pubsub.PubSubMessage;
import com.redis.spring.batch.item.redis.reader.pubsub.PubSubMessageListener;
import com.redis.spring.batch.item.redis.reader.pubsub.Subscription;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.codec.RedisCodec;

public class KeyEventListenerContainer<K, V> implements SmartLifecycle {

    private static final String KEYSPACE_PATTERN = "__keyspace@%s__:%s";

    private static final String KEYEVENT_PATTERN = "__keyevent@%s__:*";

    public static final String NOTIFY_CONFIG = "notify-keyspace-events";

    public static final String NOTIFY_CONFIG_VALUE = "KEA";

    private static final String SEPARATOR = ":";

    private final Log log = LogFactory.getLog(getClass());

    private final AbstractRedisClient client;

    private final PubSubListenerContainer<K, V> pubSubListenerContainer;

    private final Function<String, K> keyEncoder;

    private final Function<K, String> keyDecoder;

    private final Function<V, String> valueDecoder;

    public KeyEventListenerContainer(AbstractRedisClient client, RedisCodec<K, V> codec) {
        this.client = client;
        this.pubSubListenerContainer = PubSubListenerContainer.create(client, codec);
        this.keyEncoder = BatchUtils.stringKeyFunction(codec);
        this.keyDecoder = BatchUtils.toStringKeyFunction(codec);
        this.valueDecoder = BatchUtils.toStringValueFunction(codec);
    }

    @Override
    public synchronized void start() {
        if (!pubSubListenerContainer.isRunning()) {
            checkNotifyConfig();
            pubSubListenerContainer.start();
        }
    }

    @Override
    public synchronized void stop() {
        if (pubSubListenerContainer.isRunning()) {
            pubSubListenerContainer.stop();
        }
    }

    @Override
    public boolean isRunning() {
        return pubSubListenerContainer.isRunning();
    }

    public static <K, V> KeyEventListenerContainer<K, V> create(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new KeyEventListenerContainer<>(client, codec);
    }

    public Subscription receive(int database, String keyPattern, KeyEventListener<K> listener) {
        if (keyPattern == null) {
            String pattern = String.format(KEYEVENT_PATTERN, database);
            return receive(pattern, listener, this::keyEventNotification);
        }
        String pattern = String.format(KEYSPACE_PATTERN, database, keyPattern);
        return receive(pattern, listener, this::keySpaceNotification);
    }

    @SuppressWarnings("unchecked")
    private KeyEvent<K> keyEventNotification(PubSubMessage<K, V> m) {
        return keyEvent((K) m.getMessage(), suffix(m.getChannel()));
    }

    private KeyEvent<K> keySpaceNotification(PubSubMessage<K, V> m) {
        return keyEvent(keyEncoder.apply(suffix(m.getChannel())), valueDecoder.apply(m.getMessage()));
    }

    private Subscription receive(String pattern, KeyEventListener<K> listener,
            Function<PubSubMessage<K, V>, KeyEvent<K>> mapper) {
        return pubSubListenerContainer.receive(keyEncoder.apply(pattern), new KeyEventMessageListener<>(listener, mapper));
    }

    private static class KeyEventMessageListener<K, V> implements PubSubMessageListener<K, V> {

        private final KeyEventListener<K> keyEventListener;

        private final Function<PubSubMessage<K, V>, KeyEvent<K>> mapper;

        public KeyEventMessageListener(KeyEventListener<K> listener, Function<PubSubMessage<K, V>, KeyEvent<K>> mapper) {
            this.keyEventListener = listener;
            this.mapper = mapper;
        }

        @Override
        public void onMessage(PubSubMessage<K, V> message) {
            keyEventListener.onKeyEvent(mapper.apply(message));
        }

    }

    private String suffix(K channel) {
        String string = keyDecoder.apply(channel);
        int index = string.indexOf(SEPARATOR);
        if (index > 0) {
            return string.substring(index + 1);
        }
        return null;
    }

    private static <K> KeyEvent<K> keyEvent(K key, String event) {
        String type = type(event);
        KeyEvent<K> keyEvent = new KeyEvent<>();
        keyEvent.setKey(key);
        keyEvent.setEvent(event);
        keyEvent.setType(type);
        return keyEvent;
    }

    public static String type(String event) {
        if (event == null) {
            return null;
        }
        if (event.startsWith("xgroup-")) {
            return KeyValue.TYPE_STREAM;
        }
        if (event.startsWith("ts.")) {
            return KeyValue.TYPE_TIMESERIES;
        }
        if (event.startsWith("json.")) {
            return KeyValue.TYPE_JSON;
        }
        switch (event) {
            case "set":
            case "setrange":
            case "incrby":
            case "incrbyfloat":
            case "append":
                return KeyValue.TYPE_STRING;
            case "lpush":
            case "rpush":
            case "rpop":
            case "lpop":
            case "linsert":
            case "lset":
            case "lrem":
            case "ltrim":
                return KeyValue.TYPE_LIST;
            case "hset":
            case "hincrby":
            case "hincrbyfloat":
            case "hdel":
                return KeyValue.TYPE_HASH;
            case "sadd":
            case "spop":
            case "sinterstore":
            case "sunionstore":
            case "sdiffstore":
                return KeyValue.TYPE_SET;
            case "zincr":
            case "zadd":
            case "zrem":
            case "zrembyscore":
            case "zrembyrank":
            case "zdiffstore":
            case "zinterstore":
            case "zunionstore":
                return KeyValue.TYPE_ZSET;
            case "xadd":
            case "xtrim":
            case "xdel":
            case "xsetid":
                return KeyValue.TYPE_STREAM;
            default:
                return null;
        }
    }

    private void checkNotifyConfig() {
        Map<String, String> valueMap;
        try (StatefulRedisModulesConnection<String, String> connection = ConnectionBuilder.client(client).connection()) {
            try {
                valueMap = connection.sync().configGet(NOTIFY_CONFIG);
            } catch (RedisException e) {
                log.info("Could not check keyspace notification config", e);
                return;
            }
        }
        String actual = valueMap.getOrDefault(NOTIFY_CONFIG, "");
        log.info(MessageFormat.format("Retrieved config {0}: {1}", NOTIFY_CONFIG, actual));
        Set<Character> expected = characterSet(NOTIFY_CONFIG_VALUE);
        Assert.isTrue(characterSet(actual).containsAll(expected),
                String.format("Keyspace notifications not property configured. Expected %s '%s' but was '%s'.", NOTIFY_CONFIG,
                        NOTIFY_CONFIG_VALUE, actual));
    }

    private static Set<Character> characterSet(String string) {
        return string.codePoints().mapToObj(c -> (char) c).collect(Collectors.toSet());
    }

}
