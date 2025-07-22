package com.redis.riot;

import com.redis.riot.core.RedisContext;
import com.redis.riot.core.RiotVersion;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.EventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.CommandLatencyRecorder;
import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.metrics.MicrometerOptions;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

public class MetricsArgs {

    public enum MetricsOutput {
        METER, STDOUT, LOG, NONE
    }

    public static final int DEFAULT_PORT = 8080;

    public static final Duration DEFAULT_STEP = Duration.ofSeconds(1);

    public static final Duration DEFAULT_REDIS_STEP = Duration.ofSeconds(1);

    public static final boolean DEFAULT_DESCRIPTIONS = true;

    public static final String DEFAULT_NAME = "RIOTX-" + RiotVersion.getVersion();

    public static final boolean DEFAULT_JVM = true;

    public static final List<Double> DEFAULT_PERCENTILES = List.of(50d, 90d, 95d, 99d);

    @Option(names = "--metrics", defaultValue = "${RIOT_METRICS}", description = "Enable metrics.")
    private boolean enabled;

    @Option(names = "--metrics-name", description = "Application name tag that will be applied to all metrics (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    private String name = DEFAULT_NAME;

    @Option(names = "--metrics-desc", negatable = true, defaultValue = "true", description = "Enable publishing descriptions as part of the scrape payload to Prometheus. Turn off to minimize the amount of data sent on each scrape.", hidden = true)
    private boolean decriptions = DEFAULT_DESCRIPTIONS;

    @Option(names = "--metrics-jvm", defaultValue = "${RIOT_METRICS_JVM:-true}", negatable = true, fallbackValue = "true", description = "Enable/disable JVM metrics. Enabled by default.")
    private boolean jvm = DEFAULT_JVM;

    @Option(names = "--metrics-redis", defaultValue = "${RIOT_METRICS_REDIS}", description = "Command latency metrics: ${COMPLETION-CANDIDATES} (default: disabled).")
    private MetricsOutput redisOutput = MetricsOutput.NONE;

    @Option(names = "--metrics-redis-first", description = "Show first-response command latency.")
    private boolean redisFirstResponse;

    @Option(names = "--metrics-redis-step", description = "Command latency publishing interval (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
    private Duration redisStep = DEFAULT_REDIS_STEP;

    @Option(names = "--metrics-redis-perc", description = "Percentiles to display in histogram (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
    private List<Double> redisPercentiles = DEFAULT_PERCENTILES;

    @Option(names = "--metrics-step", description = "Metrics reporting interval (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>", hidden = true)
    private Duration step = DEFAULT_STEP;

    @Option(names = "--metrics-port", defaultValue = "${RIOT_METRICS_PORT:-8080}", description = "Port that Prometheus HTTP server should listen on (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int port = DEFAULT_PORT;

    @Option(names = "--metrics-prop", description = "Additional properties to pass to the Prometheus client. See https://prometheus.github.io/client_java/config/config/", paramLabel = "<k=v>")
    private Map<String, String> properties = new HashMap<>();

    public PrometheusConfig prometheusConfig() {
        return new PrometheusConfig() {

            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public boolean descriptions() {
                return decriptions;
            }

            @Override
            public Properties prometheusProperties() {
                Properties prometheusProperties = PrometheusConfig.super.prometheusProperties();
                prometheusProperties.putAll(properties);
                return prometheusProperties;

            }

            @Override
            public Duration step() {
                return step;
            }
        };
    }

    public void configure(RedisContext context) {
        context.clientResources(clientResources());
    }

    private ClientResources clientResources() {
        switch (redisOutput) {
            case NONE:
                return null;
            case METER:
                return metricsClientResources().commandLatencyRecorder(micrometerLatencyRecorder()).build();
            default:
                ClientResources resources = metricsClientResources().build();
                resources.eventBus().get().filter(e -> e instanceof CommandLatencyEvent).cast(CommandLatencyEvent.class)
                        .subscribe(commandLatencyPrinter());
                return resources;
        }
    }

    private Consumer<CommandLatencyEvent> commandLatencyPrinter() {
        CommandLatencyEventPrinter printer = new CommandLatencyEventPrinter(redisPercentiles, latencyEventPrinter(), redisStep);
        printer.setShowFirstResponse(redisFirstResponse);
        return printer;
    }

    private Consumer<String> latencyEventPrinter() {
        switch (redisOutput) {
            case LOG:
                Logger logger = LoggerFactory.getLogger(CommandLatencyEventPrinter.class);
                return logger::info;
            default:
                return System.out::println;
        }
    }

    private CommandLatencyRecorder micrometerLatencyRecorder() {
        return new MicrometerCommandLatencyRecorder(Metrics.globalRegistry, micrometerOptions());
    }

    private ClientResources.Builder metricsClientResources() {
        return ClientResources.builder().commandLatencyPublisherOptions(eventPublisherOptions());
    }

    private EventPublisherOptions eventPublisherOptions() {
        return DefaultEventPublisherOptions.builder().eventEmitInterval(redisStep).build();
    }

    @SuppressWarnings("resource")
    public void configureMetrics() throws IOException {
        if (enabled) {
            PrometheusMeterRegistry registry = new PrometheusMeterRegistry(prometheusConfig());
            registry.config().commonTags(commonTags());
            Metrics.globalRegistry.add(registry);
            if (jvm) {
                new JvmMemoryMetrics().bindTo(Metrics.globalRegistry);
                new JvmGcMetrics().bindTo(Metrics.globalRegistry);
                new JvmThreadMetrics().bindTo(Metrics.globalRegistry);
                new ProcessorMetrics().bindTo(Metrics.globalRegistry);
                new UptimeMetrics().bindTo(Metrics.globalRegistry);
            }
            startHttpServer(registry.getPrometheusRegistry());
        }
    }

    private Tags commonTags() {
        return Tags.of("application", name);
    }

    private void startHttpServer(PrometheusRegistry registry) throws IOException {
        HTTPServer.builder().port(port).registry(registry).buildAndStart();
    }

    private MicrometerOptions micrometerOptions() {
        return MicrometerOptions.builder().enable().tags(commonTags()).build();
    }

    public Duration getRedisStep() {
        return redisStep;
    }

    public void setRedisStep(Duration redisStep) {
        this.redisStep = redisStep;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setJvm(boolean enabled) {
        this.jvm = enabled;
    }

    public boolean isJvm() {
        return jvm;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Duration getStep() {
        return step;
    }

    public void setStep(Duration step) {
        this.step = step;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDecriptions(boolean decriptions) {
        this.decriptions = decriptions;
    }

    public List<Double> getRedisPercentiles() {
        return redisPercentiles;
    }

    public void setRedisPercentiles(List<Double> redisPercentiles) {
        this.redisPercentiles = redisPercentiles;
    }

    public MetricsOutput getRedisOutput() {
        return redisOutput;
    }

    public void setRedisOutput(MetricsOutput redisOutput) {
        this.redisOutput = redisOutput;
    }

}
