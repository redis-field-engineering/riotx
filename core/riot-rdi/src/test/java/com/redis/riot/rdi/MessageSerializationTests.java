package com.redis.riot.rdi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MessageSerializationTests {

    private final ObjectMapper mapper = new ObjectMapper();

    private InputStream messageJsonInputStream() {
        return getClass().getClassLoader().getResourceAsStream("message.json");
    }



    @Test
    public void testMessageSerialization() throws Exception {

        // Given
        Message message = new Message(); // Your message object
        message.setBefore(null);
        Map<String, Object> after = new HashMap<>();
        after.put("TrackId", 1);
        after.put("Name", "For Those About To Rock (We Salute You)");
        after.put("AlbumId", 1);
        after.put("MediaTypeId", 1);
        after.put("GenreId", 1);
        after.put("Composer", "Angus Young, Malcolm Young, Brian Johnson");
        after.put("Milliseconds", 343719);
        after.put("Bytes", 11170334);
        after.put("UnitPrice", "0.99");
        message.setAfter(after);
        Message.Source source = new Message.Source();
        source.setVersion("2.7.3.Final");
        source.setConnector("postgresql");
        source.setName("rdi");
        source.setTs_ms(1740785606068L);
        source.setSnapshot("first_in_data_collection");
        source.setDb("chinook");
        source.setSequence(null);
        source.setTs_us(1740785606068813L);
        source.setTs_ns(1740785606068813000L);
        source.setSchema("public");
        source.setTable("Track");
        source.setTxId(766);
        source.setLsn(26447848);
        source.setXmin(null);
        message.setSource(source);
        message.setTransaction(null);
        message.setOp(Message.Operation.READ);
        message.setTs_ms(1740785606297L);
        message.setTs_us(1740785606297446L);
        message.setTs_ns(1740785606297446000L);
        // When
        JsonNode actualJson = mapper.readTree(mapper.writeValueAsString(message));
        JsonNode expectedJson = mapper.readTree(messageJsonInputStream());

        // Then - Full comparison
        assertThat(actualJson).isEqualTo(expectedJson);

    }

    @Test
    public void testMessageDeserialization() throws Exception {
        // Given
        ObjectMapper mapper = new ObjectMapper();
        String json = new String(messageJsonInputStream().readAllBytes());

        // When
        Message message = mapper.readValue(json, Message.class);

        // Then
        // Assertions on the deserialized object
        assertThat(message.getAfter()).isNotNull();
        assertThat(message.getOp()).isEqualTo(Message.Operation.READ);
    }
}
