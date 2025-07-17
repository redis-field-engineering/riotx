package com.redis.riot;

import java.util.Map;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(usageHelpAutoWidth = true, abbreviateSynopsis = true)
public class BaseCommand {

    static {
        if (System.getenv().containsKey("RIOT_NO_COLOR")) {
            System.setProperty("picocli.ansi", "false");
        }
    }

    @Spec
    CommandSpec commandSpec;

    @Mixin
    LoggingMixin loggingMixin;

    @Option(names = "--help", description = "Show this help message and exit.", usageHelp = true)
    boolean help;

    @Option(names = { "-V", "--version" }, description = "Print version information and exit.", versionHelp = true)
    boolean version;

    @Option(names = "-D", paramLabel = "<key=value>", description = "Sets a System property.", mapFallbackValue = "", hidden = true)
    void setProperty(Map<String, String> props) {
        props.forEach(System::setProperty);
    }

    public CommandSpec getCommandSpec() {
        return commandSpec;
    }

    public void setCommandSpec(CommandSpec commandSpec) {
        this.commandSpec = commandSpec;
    }

    public LoggingMixin getLoggingMixin() {
        return loggingMixin;
    }

    public void setLoggingMixin(LoggingMixin loggingMixin) {
        this.loggingMixin = loggingMixin;
    }

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public boolean isVersion() {
        return version;
    }

    public void setVersion(boolean version) {
        this.version = version;
    }

}
