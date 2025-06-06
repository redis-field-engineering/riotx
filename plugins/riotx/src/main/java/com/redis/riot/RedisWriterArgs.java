package com.redis.riot;

import java.time.Duration;

import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.Wait;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite.WriteMode;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class RedisWriterArgs {

    public static final Duration DEFAULT_WAIT_TIMEOUT = Wait.DEFAULT_TIMEOUT;

    @Option(names = "--multi-exec", description = "Enable MULTI/EXEC writes.")
    private boolean multiExec;

    @Option(names = "--wait-replicas", description = "Number of replicas for WAIT command (default: 0 i.e. no wait).", paramLabel = "<int>")
    private int waitReplicas;

    @Option(names = "--wait-timeout", description = "Timeout for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;

    @Option(names = "--merge", defaultValue = "${RIOT_MERGE}", description = "Merge collection data structures (hash, list, ...) instead of overwriting them. Only used for Redis data-structure replication and import.")
    private boolean merge;

    public <K, V, T> void configure(RedisItemWriter<K, V, T> writer) {
        writer.setMultiExec(multiExec);
        writer.setWait(redisWait());
        writer.setMode(writeMode());
    }

    private WriteMode writeMode() {
        return merge ? WriteMode.MERGE : WriteMode.OVERWRITE;
    }

    private Wait redisWait() {
        Wait wait = new Wait();
        wait.setReplicas(waitReplicas);
        wait.setTimeout(waitTimeout);
        return wait;
    }

    public boolean isMultiExec() {
        return multiExec;
    }

    public void setMultiExec(boolean multiExec) {
        this.multiExec = multiExec;
    }

    public int getWaitReplicas() {
        return waitReplicas;
    }

    public void setWaitReplicas(int waitReplicas) {
        this.waitReplicas = waitReplicas;
    }

    public Duration getWaitTimeout() {
        return waitTimeout;
    }

    public void setWaitTimeout(Duration waitTimeout) {
        this.waitTimeout = waitTimeout;
    }

    public boolean isMerge() {
        return merge;
    }

    public void setMerge(boolean merge) {
        this.merge = merge;
    }

}
