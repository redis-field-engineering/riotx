package com.redis.riot;

import com.redis.riot.core.job.RiotStep;
import com.redis.spring.batch.step.FlushingChunkProvider;
import picocli.CommandLine;

import java.time.Duration;

public class FlushingStepArgs implements StepConfigurer {

    public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

    @CommandLine.Option(names = "--flush", defaultValue = "${RIOT_FLUSH:-50ms}", description = "Max duration between batch flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

    @CommandLine.Option(names = "--idle", defaultValue = "${RIOT_IDLE}", description = "Min duration to consider reader complete, for example 3s 5m (default: no timeout).", paramLabel = "<dur>")
    private Duration idleTimeout;

    @Override
    public void configure(RiotStep<?, ?> step) {
        step.setFlushInterval(flushInterval);
        step.setIdleTimeout(idleTimeout);
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(Duration flushInterval) {
        this.flushInterval = flushInterval;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

}
