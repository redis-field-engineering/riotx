package com.redis.riot.operation;

import lombok.ToString;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.time.Instant;

@ToString
public class ExpireTtlArgs {

    @Option(names = "--ttl-field", description = "Expire timeout millis field.", paramLabel = "<field>")
    private String ttlField;

    @Option(names = "--ttl", description = "Expire timeout duration.", paramLabel = "<dur>")
    private Duration ttl;

    @Option(names = "--time-field", description = "Expire POSIX millis time field.", paramLabel = "<field>")
    private String timeField;

    @Option(names = "--time", description = "Expire time.", paramLabel = "<time>")
    private Instant time;

    public String getTtlField() {
        return ttlField;
    }

    public void setTtlField(String ttlField) {
        this.ttlField = ttlField;
    }

    public Duration getTtl() {
        return ttl;
    }

    public void setTtl(Duration value) {
        this.ttl = value;
    }

    public String getTimeField() {
        return timeField;
    }

    public void setTimeField(String timeField) {
        this.timeField = timeField;
    }

    public Instant getTime() {
        return time;
    }

    public void setTime(Instant time) {
        this.time = time;
    }

}
