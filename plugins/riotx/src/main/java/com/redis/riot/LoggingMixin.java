package com.redis.riot;

import static picocli.CommandLine.Spec.Target.MIXEE;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.event.Level;
import org.slf4j.simple.SimpleLogger;

import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.Spec;

public class LoggingMixin {

    public static final Level DEFAULT_LEVEL = Level.WARN;

    /**
     * This mixin is able to climb the command hierarchy because the {@code @Spec(Target.MIXEE)}-annotated field gets a
     * reference to the command where it is used.
     */
    private @Spec(MIXEE) CommandSpec mixee; // spec of the command where the @Mixin is used

    private String file;

    private boolean hideDateTime;

    private String dateTimeFormat;

    private boolean showThreadId;

    private boolean showThreadName;

    private boolean showLogName;

    private boolean showShortLogName;

    private boolean levelInBrackets;

    private Map<String, Level> levels = new LinkedHashMap<>();

    private Level level = DEFAULT_LEVEL;

    private static LoggingMixin getTopLevelMixin(CommandSpec commandSpec) {
        return ((MainCommand) commandSpec.root().userObject()).loggingMixin;
    }

    @Option(names = "--log-file", description = "Write logs to a file.", paramLabel = "<file>")
    public void setFile(String file) {
        mixin().file = file;
    }

    @Option(names = "--no-log-time", description = "Do not show current date and time in log messages.")
    public void setHideDateTime(boolean hide) {
        mixin().hideDateTime = hide;
    }

    @Option(names = "--log-time-fmt", defaultValue = "yyyy-MM-dd HH:mm:ss.SSS", description = "Date and time format to be used in log messages (default: ${DEFAULT-VALUE}). Use with --log-time.", paramLabel = "<f>")
    public void setDateTimeFormat(String format) {
        mixin().dateTimeFormat = format;
    }

    @Option(names = "--log-thread-id", description = "Show current thread ID in log messages.", hidden = true)
    public void setShowThreadId(boolean show) {
        mixin().showThreadId = show;
    }

    @Option(names = "--log-thread", description = "Show current thread name in log messages.", hidden = true)
    public void setShowThreadName(boolean show) {
        mixin().showThreadName = show;
    }

    @Option(names = "--log-name", description = "Show logger instance name in log messages.", hidden = true)
    public void setShowLogName(boolean show) {
        mixin().showLogName = show;
    }

    @Option(names = "--log-short", description = "Only show last component of logger instance name in log messages.", hidden = true)
    public void setShowShortLogName(boolean show) {
        mixin().showShortLogName = show;
    }

    @Option(names = "--log-level-brck", description = "Output log level string in brackets.", hidden = true)
    public void setLevelInBrackets(boolean enable) {
        mixin().levelInBrackets = enable;
    }

    @Option(arity = "1..*", names = "--log", description = "Custom log levels (e.g.: io.lettuce=INFO).", paramLabel = "<lvl>")
    public void setLevels(Map<String, Level> levels) {
        mixin().levels = levels;
    }

    @Option(names = { "-d", "--debug" }, description = "Log in debug mode.")
    public void setDebug(boolean enable) {
        if (enable) {
            mixin().level = Level.DEBUG;
        }
    }

    @Option(names = { "-i", "--info" }, description = "Set log level to info.")
    public void setInfo(boolean enable) {
        if (enable) {
            mixin().level = Level.INFO;
        }
    }

    @Option(names = { "-q", "--quiet" }, description = "Log errors only.")
    public void setError(boolean enable) {
        if (enable) {
            mixin().level = Level.ERROR;
        }
    }

    @Option(names = "--log-level", defaultValue = "${RIOT_LOG}", description = "Set log level: ${COMPLETION-CANDIDATES} (default: WARN).", paramLabel = "<lvl>")
    public void setLevel(Level level) {
        mixin().level = level;
    }

    public static int executionStrategy(ParseResult parseResult) {
        getTopLevelMixin(parseResult.commandSpec()).configureLoggers();
        return 0;
    }

    public void configureLoggers() {
        LoggingMixin mixin = mixin();
        System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, mixin.level.name());
        if (mixin.file != null) {
            System.setProperty(SimpleLogger.LOG_FILE_KEY, mixin.file);
        }
        setBoolean(SimpleLogger.SHOW_DATE_TIME_KEY, !mixin.hideDateTime);
        if (mixin.dateTimeFormat != null) {
            System.setProperty(SimpleLogger.DATE_TIME_FORMAT_KEY, mixin.dateTimeFormat);
        }
        setBoolean(SimpleLogger.SHOW_THREAD_ID_KEY, mixin.showThreadId);
        setBoolean(SimpleLogger.SHOW_THREAD_NAME_KEY, mixin.showThreadName);
        setBoolean(SimpleLogger.SHOW_LOG_NAME_KEY, mixin.showLogName);
        setBoolean(SimpleLogger.SHOW_SHORT_LOG_NAME_KEY, mixin.showShortLogName);
        setBoolean(SimpleLogger.LEVEL_IN_BRACKETS_KEY, mixin.levelInBrackets);
        setLogLevel("com.redis.riot.CommandLatencyEventPrinter", Level.INFO);
        setLogLevel("com.redis.riot.ProgressStepExecutionListener", Level.INFO);
        setLogLevel("com.amazonaws.internal", Level.ERROR);
        setLogLevel("com.redis.spring.batch.step.FlushingFaultTolerantStepBuilder", Level.ERROR);
        setLogLevel("org.springframework.batch.core.step.builder.FaultTolerantStepBuilder", Level.ERROR);
        setLogLevel("org.springframework.batch.core.step.item.ChunkMonitor", Level.ERROR);
        setLogLevel("net.snowflake", Level.WARN);
        setLogLevel("net.snowflake.client.core.FileUtil", "OFF");
        setLogLevel("io.netty.resolver.dns", Level.ERROR);
        mixin.levels.forEach(this::setLogLevel);
    }

    private void setLogLevel(String key, Level level) {
        setLogLevel(key, level.name());
    }

    private void setLogLevel(String key, String level) {
        System.setProperty(SimpleLogger.LOG_KEY_PREFIX + key, level);
    }

    private void setBoolean(String property, boolean value) {
        System.setProperty(property, String.valueOf(value));
    }

    private LoggingMixin mixin() {
        return getTopLevelMixin(mixee);
    }

    public boolean isStacktrace() {
        return mixin().level.toInt() <= Level.INFO.toInt();
    }

}
