package com.redis.riot;

import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.CommandLatencyId;
import io.lettuce.core.metrics.CommandMetrics;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CommandLatencyEventPrinter implements Consumer<CommandLatencyEvent> {

    private final List<Double> percentiles;

    private final Consumer<String> printer;

    private final Duration interval;

    private boolean showFirstResponse;

    public CommandLatencyEventPrinter(List<Double> percentiles, Consumer<String> printer, Duration interval) {
        this.percentiles = percentiles;
        this.printer = printer;
        this.interval = interval;
    }

    public boolean isShowFirstResponse() {
        return showFirstResponse;
    }

    public void setShowFirstResponse(boolean showFirstResponse) {
        this.showFirstResponse = showFirstResponse;
    }

    @Override
    public void accept(CommandLatencyEvent event) {
        event.getLatencies().forEach(this::printEvent);
    }

    private void printEvent(CommandLatencyId id, CommandMetrics metrics) {
        new MetricsPrinter(id, metrics).print();
    }

    private void printf(String format, Object... args) {
        printer.accept(String.format(format, args));
    }

    private class MetricsPrinter {

        private final CommandLatencyId id;

        private final CommandMetrics metrics;

        private MetricsPrinter(CommandLatencyId id, CommandMetrics metrics) {
            this.id = id;
            this.metrics = metrics;
        }

        public void print() {
            long throughput = metrics.getCount() / interval.toSeconds();
            printf("[%s] %,d ops/s, %s", id.commandType(), throughput, toString(metrics.getCompletion()));
            if (showFirstResponse) {
                printf("[%s] first %s", id.commandType(), toString(metrics.getCompletion()));
            }
        }

        private String toString(Map<Double, Long> completionPercentiles) {
            return percentiles.stream().map(p -> String.format("p%.0f=%s", p, formatLatency(convertToMs(completionPercentiles.get(p)))))
                    .collect(Collectors.joining(", "));
        }

        private String toString(CommandMetrics.CommandLatency latency) {
            if (latency == null) {
                return "N/A";
            }
            double minMs = convertToMs(latency.getMin());
            double maxMs = convertToMs(latency.getMax());
            return String.format("min=%s, max=%s, %s", formatLatency(minMs), formatLatency(maxMs),
                    toString(latency.getPercentiles()));
        }

        private double convertToMs(long value) {
            return TimeUnit.NANOSECONDS.toMicros(metrics.getTimeUnit().toNanos(value)) / 1000.0;
        }

        private String formatLatency(double latencyMs) {
            if (latencyMs < 10.0) {
                return String.format("%.1f", latencyMs);
            } else {
                return String.format("%.0f", latencyMs);
            }
        }

    }

}
