package com.redis.riot;

import com.redis.riot.core.StreamLengthBackpressureStatusSupplier;
import picocli.CommandLine;

public class DebeziumStreamArgs {

    public static final String DEFAULT_STREAM_PREFIX = "data:";

    public static final long DEFAULT_STREAM_LIMIT = StreamLengthBackpressureStatusSupplier.DEFAULT_LIMIT;

    @CommandLine.Option(names = "--stream-prefix", defaultValue = "${RIOT_STREAM_PREFIX:-data:}", description = "Key prefix for stream containing change events (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
    private String streamPrefix = DEFAULT_STREAM_PREFIX;

    @CommandLine.Option(names = "--stream-limit", defaultValue = "${RIOT_STREAM_LIMIT}", description = "Max length of RDI stream (default: ${DEFAULT-VALUE}). Use 0 for no limit.", paramLabel = "<int>")
    private long streamLimit = DEFAULT_STREAM_LIMIT;

    public String getStreamPrefix() {
        return streamPrefix;
    }

    public void setStreamPrefix(String streamPrefix) {
        this.streamPrefix = streamPrefix;
    }

    public long getStreamLimit() {
        return streamLimit;
    }

    public void setStreamLimit(long streamLimit) {
        this.streamLimit = streamLimit;
    }

}
