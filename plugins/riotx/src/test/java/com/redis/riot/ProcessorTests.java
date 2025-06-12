package com.redis.riot;

import com.redis.batch.KeyType;
import com.redis.batch.KeyValue;
import com.redis.batch.Range;
import com.redis.riot.core.function.KeyValueMap;
import com.redis.riot.core.function.StringToMapFunction;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.StringCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessorTests {

    private Predicate<String> keyFilter(KeyFilterArgs args) {
        return args.predicate(StringCodec.UTF8);
    }

    @Test
    void keyFilter() {
        KeyFilterArgs options = new KeyFilterArgs();
        options.setIncludes(Arrays.asList("foo*", "bar*"));
        Predicate<String> predicate = keyFilter(options);
        Assertions.assertTrue(predicate.test("foobar"));
        Assertions.assertTrue(predicate.test("barfoo"));
        Assertions.assertFalse(predicate.test("key"));
    }

    @Test
    void slotExact() {
        KeyFilterArgs options = new KeyFilterArgs();
        options.setSlots(Collections.singletonList(new Range(7638, 7638)));
        Predicate<String> predicate = keyFilter(options);
        assertTrue(predicate.test("abc"));
        assertFalse(predicate.test("abcd"));
    }

    @Test
    void slotRange() {
        KeyFilterArgs options = new KeyFilterArgs();
        options.setSlots(slotRangeList(0, SlotHash.SLOT_COUNT));
        Predicate<String> unbounded = keyFilter(options);
        assertTrue(unbounded.test("foo"));
        assertTrue(unbounded.test("foo1"));
        options.setSlots(slotRangeList(999999, 999999));
        Predicate<String> is999999 = keyFilter(options);
        assertFalse(is999999.test("foo"));
    }

    private List<Range> slotRangeList(int start, int end) {
        return Collections.singletonList(new Range(start, end));
    }

    @Test
    void kitchenSink() {
        KeyFilterArgs options = new KeyFilterArgs();
        options.setExcludes(Collections.singletonList("foo"));
        options.setIncludes(Collections.singletonList("foo1"));
        options.setSlots(Collections.singletonList(new Range(0, SlotHash.SLOT_COUNT)));
        Predicate<String> predicate = keyFilter(options);
        assertFalse(predicate.test("foo"));
        assertFalse(predicate.test("bar"));
        assertTrue(predicate.test("foo1"));
    }

    @Test
    void keyValueToMap() {
        KeyValueMap processor = new KeyValueMap();
        KeyValue<String> string = new KeyValue<>();
        string.setKey("beer:1");
        string.setType(KeyType.STRING.getString());
        String value = "sdfsdf";
        string.setValue(value);
        Map<String, ?> stringMap = processor.apply(string);
        Assertions.assertEquals(value, stringMap.get(StringToMapFunction.DEFAULT_KEY));
        KeyValue<String> hash = new KeyValue<>();
        hash.setKey("beer:2");
        hash.setType(KeyType.HASH.getString());
        Map<String, String> map = new HashMap<>();
        map.put("field1", "value1");
        hash.setValue(map);
        Map<String, ?> hashMap = processor.apply(hash);
        Assertions.assertEquals("value1", hashMap.get("field1"));
    }

}
