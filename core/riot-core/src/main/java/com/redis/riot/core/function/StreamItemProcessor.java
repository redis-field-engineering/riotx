package com.redis.riot.core.function;

import com.redis.batch.KeyStructEvent;
import com.redis.batch.KeyType;
import io.lettuce.core.StreamMessage;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.stream.Collectors;

public class StreamItemProcessor implements ItemProcessor<KeyStructEvent<String, String>, KeyStructEvent<String, String>> {

    private boolean prune;

    private boolean dropMessageIds;

    @Override
    public KeyStructEvent<String, String> process(KeyStructEvent<String, String> t) {
        if (t.getValue() == null || t.getType() != KeyType.stream) {
            return t;
        }
        Collection<StreamMessage<String, String>> messages = t.asStream();
        if (CollectionUtils.isEmpty(messages)) {
            if (prune) {
                return null;
            }
        } else {
            if (dropMessageIds) {
                t.setValue(messages.stream().map(this::message).collect(Collectors.toList()));
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
