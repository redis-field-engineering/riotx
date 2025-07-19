package com.redis.riot;

import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.Callable;

import com.redis.riot.core.Expression;
import com.redis.riot.core.InetSocketAddressList;
import com.redis.riot.core.PrefixedNumber;
import com.redis.riot.core.TemplateExpression;
import com.redis.riot.db.DatabaseObject;
import com.redis.batch.Range;
import io.lettuce.core.RedisURI;
import io.lettuce.core.json.JsonPath;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.util.unit.DataSize;

import picocli.CommandLine;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunLast;
import software.amazon.awssdk.regions.Region;

public class MainCommand extends BaseCommand implements Callable<Integer>, IO {

    private PrintWriter out;

    private PrintWriter err;

    private boolean disableExceptionMessageHandling;

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
        commandLine.setAllowSubcommandsAsOptionParameters(true);
        commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
        if (!disableExceptionMessageHandling) {
            commandLine.setExecutionExceptionHandler(new PrintExceptionMessageHandler());
        }
        commandLine.registerConverter(Duration.class, DurationStyle.SIMPLE::parse);
        commandLine.registerConverter(DataSize.class, MainCommand::parseDataSize);
        commandLine.registerConverter(Expression.class, Expression::parse);
        commandLine.registerConverter(TemplateExpression.class, Expression::parseTemplate);
        commandLine.registerConverter(NumberFormat.class, java.text.DecimalFormat::new);
        commandLine.registerConverter(DateFormat.class, SimpleDateFormat::new);
        commandLine.registerConverter(RedisURI.class, new RedisURIConverter());
        commandLine.registerConverter(Region.class, Region::of);
        commandLine.registerConverter(Range.class, new RangeConverter());
        commandLine.registerConverter(JsonPath.class, JsonPath::of);
        commandLine.registerConverter(DatabaseObject.class, DatabaseObject::parse);
        commandLine.registerConverter(InetSocketAddressList.class, InetSocketAddressList::parse);
        commandLine.registerConverter(PrefixedNumber.class, PrefixedNumber::parse);
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

    public boolean isDisableExceptionMessageHandling() {
        return disableExceptionMessageHandling;
    }

    public void setDisableExceptionMessageHandling(boolean disableExceptionMessageHandling) {
        this.disableExceptionMessageHandling = disableExceptionMessageHandling;
    }

}
