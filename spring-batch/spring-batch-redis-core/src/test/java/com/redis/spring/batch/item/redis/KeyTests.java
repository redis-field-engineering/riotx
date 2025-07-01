package com.redis.spring.batch.item.redis;

import com.redis.batch.KeyStructEvent;
import com.redis.batch.KeyType;
import com.redis.spring.batch.item.redis.reader.KeyStructComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.StringCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class KeyTests {

    @Test
    void compareStreamMessageId() {
        KeyStructComparator<String, String> comparator = new KeyStructComparator<>(StringCodec.UTF8);
        String key = "key:1";
        KeyType type = KeyType.stream;
        String messageId = "12345";
        Map<String, String> body = new HashMap<>();
        KeyStructEvent<String, String> kv1 = new KeyStructEvent<>();
        kv1.setKey(key);
        kv1.setType(type);
        kv1.setValue(Collections.singletonList(new StreamMessage<>(key, messageId, body)));
        KeyStructEvent<String, String> kv2 = new KeyStructEvent<>();
        kv2.setKey(key);
        kv2.setType(type);
        StreamMessage<String, String> message2 = new StreamMessage<>(key, messageId + "1", body);
        kv2.setValue(Collections.singletonList(message2));
        Assertions.assertEquals(Status.VALUE, comparator.compare(kv1, kv2).getStatus());
        comparator.getValueComparator().setIgnoreStreamMessageIds(true);
        Assertions.assertEquals(Status.OK, comparator.compare(kv1, kv2).getStatus());
    }

}
