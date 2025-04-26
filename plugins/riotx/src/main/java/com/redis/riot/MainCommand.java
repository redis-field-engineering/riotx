package com.redis.riot;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.concurrent.Callable;

import com.redis.riot.core.Expression;
import com.redis.riot.core.TemplateExpression;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.util.unit.DataSize;

import picocli.CommandLine;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunLast;

public class MainCommand extends BaseCommand implements Callable<Integer>, IO {

    private PrintWriter out;

    private PrintWriter err;

    @Override
    public Integer call() throws Exception {
        commandSpec.commandLine().usage(out);
        return 0;
    }

    public int run(String... args) {
        return commandLine().execute(args);
    }

    protected CommandLine commandLine() {
        CommandLine commandLine = new CommandLine(this);
        setOut(commandLine.getOut());
        setErr(commandLine.getErr());
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
        commandLine.setExecutionExceptionHandler(new PrintExceptionMessageHandler());
        commandLine.registerConverter(Duration.class, DurationStyle.SIMPLE::parse);
        commandLine.registerConverter(DataSize.class, MainCommand::parseDataSize);
        commandLine.registerConverter(Expression.class, Expression::parse);
        commandLine.registerConverter(TemplateExpression.class, Expression::parseTemplate);
        commandLine.setExecutionStrategy(
                new CompositeExecutionStrategy(LoggingMixin::executionStrategy, this::executionStrategy));
        return commandLine;
    }

    protected int executionStrategy(ParseResult parseResult) {
        return new RunLast().execute(parseResult);
    }

    public static DataSize parseDataSize(String string) {
        return DataSize.parse(string.toUpperCase());
    }

    @Override
    public PrintWriter getOut() {
        return out;
    }

    @Override
    public void setOut(PrintWriter out) {
        this.out = out;
    }

    @Override
    public PrintWriter getErr() {
        return err;
    }

    @Override
    public void setErr(PrintWriter err) {
        this.err = err;
    }

}
