package com.redis.batch;

import java.time.Instant;

public class KeyEvent<K> extends Key<K> {

    public static final String TYPE_NONE = "none";

    public static final String TYPE_HASH = "hash";

    public static final String TYPE_JSON = "ReJSON-RL";

    public static final String TYPE_LIST = "list";

    public static final String TYPE_SET = "set";

    public static final String TYPE_STREAM = "stream";

    public static final String TYPE_STRING = "string";

    public static final String TYPE_TIMESERIES = "TSDB-TYPE";

    public static final String TYPE_ZSET = "zset";

    private String event;

    private Instant timestamp = Instant.now();

    private String type;

    /**
     * @return the code that originated this event (e.g. scan, del, ...)
     */
    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * @return POSIX time in milliseconds when the event happened
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant time) {
        this.timestamp = time;
    }

}
