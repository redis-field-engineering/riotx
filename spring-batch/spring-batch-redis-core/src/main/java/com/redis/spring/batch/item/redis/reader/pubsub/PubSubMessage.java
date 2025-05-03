package com.redis.spring.batch.item.redis.reader.pubsub;

public class PubSubMessage<K, V> {

    private K channel;

    private V message;

    public K getChannel() {
        return channel;
    }

    public void setChannel(K channel) {
        this.channel = channel;
    }

    public V getMessage() {
        return message;
    }

    public void setMessage(V message) {
        this.message = message;
    }

    public static <K, V> PubSubMessage<K, V> of(K channel, V message) {
        PubSubMessage<K, V> msg = new PubSubMessage<>();
        msg.setChannel(channel);
        msg.setMessage(message);
        return msg;
    }

}
