package com.redis.riot;

import com.redis.riot.core.RedisContext;
import com.redis.riot.core.RiotVersion;
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
import picocli.CommandLine.Option;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MetricsArgs {

    public static final int DEFAULT_PORT = 8080;

    public static final Duration DEFAULT_STEP = Duration.ofSeconds(1);

    public static final boolean DEFAULT_DESCRIPTIONS = true;

    public static final String DEFAULT_NAME = "RIOTX-" + RiotVersion.getVersion();

    @Option(names = "--metrics", description = "Enable metrics.")
    private boolean enabled;

    @Option(names = "--metrics-name", description = "Application name tag that will be applied to all metrics (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    private String name = DEFAULT_NAME;

    @Option(names = "--metrics-desc", negatable = true, defaultValue = "true", description = "Enable publishing descriptions as part of the scrape payload to Prometheus. Turn off to minimize the amount of data sent on each scrape.", hidden = true)
    private boolean decriptions = DEFAULT_DESCRIPTIONS;

    @Option(names = "--metrics-jvm", negatable = true, defaultValue = "true", fallbackValue = "true", description = "Enable/disable JVM metrics. Enabled by default.")
    private boolean jvm;

    @Option(names = "--metrics-redis", description = "Enable command latency metrics. See https://github.com/redis/lettuce/wiki/Command-Latency-Metrics#micrometer")
    private boolean redis;

    @Option(names = "--metrics-step", description = "Metrics reporting interval (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>", hidden = true)
    private Duration step = DEFAULT_STEP;

    @Option(names = "--metrics-port", description = "Port that Prometheus HTTP server should listen on (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
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

    public void setEnabled(boolean enabled) throws IOException {
        this.enabled = enabled;
    }

    public void setJvm(boolean enabled) {
        this.jvm = enabled;
    }

    public boolean isRedis() {
        return redis;
    }

    public void setRedis(boolean enable) {
        this.redis = enable;
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

    public void configure(RedisContext context) {
        if (redis) {
            MicrometerCommandLatencyRecorder recorder = new MicrometerCommandLatencyRecorder(Metrics.globalRegistry,
                    micrometerOptions());
            context.clientResources(ClientResources.builder().commandLatencyRecorder(recorder).build());
        }
    }

    @SuppressWarnings("resource")
    public void configureMetrics() throws IOException {
        if (!enabled) {
            return;
        }
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

    private Tags commonTags() {
        return Tags.of("application", name);
    }

    private HTTPServer startHttpServer(PrometheusRegistry registry) throws IOException {
        return HTTPServer.builder().port(8080).registry(registry).buildAndStart();
    }

    private MicrometerOptions micrometerOptions() {
        return MicrometerOptions.builder().enable().tags(commonTags()).build();
    }

}
