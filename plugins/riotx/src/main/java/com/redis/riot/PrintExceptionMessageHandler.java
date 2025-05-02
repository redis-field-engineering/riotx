package com.redis.riot;

import org.springframework.util.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.IExecutionExceptionHandler;
import picocli.CommandLine.ParseResult;

public class PrintExceptionMessageHandler implements IExecutionExceptionHandler {

    public int handleExecutionException(Exception exception, CommandLine cmd, ParseResult parseResult) {

        if (cmd.getCommand() instanceof BaseCommand) {
            if (((BaseCommand) cmd.getCommand()).loggingMixin.isStacktrace()) {
                exception.printStackTrace(cmd.getErr());
            }
        }

        if (StringUtils.hasLength(exception.getMessage())) {
            // bold red error message
            cmd.getErr().println(cmd.getColorScheme().errorText(exception.getMessage()));
        }

        return cmd.getExitCodeExceptionMapper() != null
                ? cmd.getExitCodeExceptionMapper().getExitCode(exception)
                : cmd.getCommandSpec().exitCodeOnExecutionException();
    }

}
