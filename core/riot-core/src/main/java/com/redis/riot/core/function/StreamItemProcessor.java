package com.redis.riot.core.function;

import java.util.Collection;
import java.util.stream.Collectors;

import com.redis.batch.KeyType;
import com.redis.batch.KeyValueEvent;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.CollectionUtils;

import io.lettuce.core.StreamMessage;

public class StreamItemProcessor implements ItemProcessor<KeyValueEvent<String>, KeyValueEvent<String>> {

    private boolean prune;

    private boolean dropMessageIds;

    @SuppressWarnings("unchecked")
    @Override
    public KeyValueEvent<String> process(KeyValueEvent<String> t) {
        if (t.getValue() != null && KeyType.STREAM.getString().equalsIgnoreCase(t.getType())) {
            Collection<StreamMessage<?, ?>> messages = (Collection<StreamMessage<?, ?>>) t.getValue();
            if (CollectionUtils.isEmpty(messages)) {
                if (prune) {
                    return null;
                }
            } else {
                if (dropMessageIds) {
                    t.setValue(messages.stream().map(this::message).collect(Collectors.toList()));
                }
            }
        }
        return t;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private StreamMessage message(StreamMessage message) {
        return new StreamMessage(message.getStream(), null, message.getBody());
    }

    public boolean isDropMessageIds() {
        return dropMessageIds;
    }

    public void setDropMessageIds(boolean dropMessageIds) {
        this.dropMessageIds = dropMessageIds;
    }

    public boolean isPrune() {
        return prune;
    }

    public void setPrune(boolean prune) {
        this.prune = prune;
    }

}
