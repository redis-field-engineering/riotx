package com.redis.batch;

import java.time.Duration;

import lombok.ToString;

@ToString
public class Wait {

    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1);

    private int replicas;

    private Duration timeout = DEFAULT_TIMEOUT;

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public static Wait of(int replicas, Duration timeout) {
        Wait wait = new Wait();
        wait.setReplicas(replicas);
        wait.setTimeout(timeout);
        return wait;
    }

}
