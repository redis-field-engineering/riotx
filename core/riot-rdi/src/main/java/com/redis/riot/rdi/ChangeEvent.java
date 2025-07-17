package com.redis.riot.rdi;

import com.redis.riot.core.RiotVersion;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ChangeEvent {

    public static final String DEFAULT_SNAPSHOT = "false";

    public static final String DEFAULT_SOURCE_NAME = "RIOT-X";

    public static final String DEFAULT_CONNECTOR_NAME = DEFAULT_SOURCE_NAME;

    public static final ChangeEventValue.Operation DEFAULT_OPERATION = ChangeEventValue.Operation.CREATE;

    private Object key;

    private ChangeEventValue value;

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public ChangeEventValue getValue() {
        return value;
    }

    public void setValue(ChangeEventValue value) {
        this.value = value;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Object key;

        private Map<String, Object> columns;

        private Instant instant = Instant.now();

        private String connector = DEFAULT_CONNECTOR_NAME;

        private String sourceName = DEFAULT_SOURCE_NAME;

        private ChangeEventValue.Operation operation = DEFAULT_OPERATION;

        private String database;

        private String schema;

        private String table;

        private String snapshot = DEFAULT_SNAPSHOT;

        public ChangeEvent build() {
            ChangeEvent changeEvent = new ChangeEvent();
            changeEvent.setKey(key);
            changeEvent.setValue(value());
            return changeEvent;
        }

        public Builder key(Object key) {
            this.key = key;
            return this;
        }

        public Builder columns(Map<String, Object> columns) {
            this.columns = columns;
            return this;
        }

        public Builder instant(Instant instant) {
            this.instant = instant;
            return this;
        }

        public Builder snapshot(String snapshot) {
            this.snapshot = snapshot;
            return this;
        }

        public Builder connector(String connector) {
            this.connector = connector;
            return this;
        }

        public Builder source(String name) {
            this.sourceName = name;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder operation(ChangeEventValue.Operation operation) {
            this.operation = operation;
            return this;
        }

        private ChangeEventValue value() {
            ChangeEventValue value = new ChangeEventValue();
            if (operation == ChangeEventValue.Operation.DELETE) {
                value.setBefore(columns);
            } else {
                value.setAfter(columns);
            }
            value.setOp(operation);
            value.setTs_ms(instant.toEpochMilli());
            value.setTs_us(micros(instant));
            value.setTs_ns(nanos(instant));
            value.setSource(source());
            return value;
        }

        private ChangeEventValue.Source source() {
            ChangeEventValue.Source source = new ChangeEventValue.Source();
            source.setConnector(connector);
            source.setVersion(RiotVersion.getVersion());
            source.setSnapshot(snapshot);
            source.setName(sourceName);
            source.setTs_ms(instant.toEpochMilli());
            source.setTs_us(micros(instant));
            source.setTs_ns(nanos(instant));
            source.setDb(database);
            source.setSchema(schema);
            source.setTable(table);
            return source;
        }

    }

    static long nanos(Instant instant) {
        return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
    }

    static long micros(Instant instant) {
        return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
    }

}
