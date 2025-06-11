package com.redis.riot.operation;

import com.redis.batch.operation.Xadd;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

@Command(name = "xadd", description = "Append entries to a stream")
public class XaddCommand extends AbstractOperationCommand {

    @ArgGroup(exclusive = false)
    private FieldFilterArgs fieldFilterArgs = new FieldFilterArgs();

    @Option(names = "--maxlen", description = "Stream maxlen.", paramLabel = "<int>")
    private long maxlen;

    @Option(names = "--trim", description = "Stream efficient trimming ('~' flag).")
    private boolean approximateTrimming;

    private XAddArgs xAddArgs() {
        XAddArgs args = new XAddArgs();
        if (maxlen > 0) {
            args.maxlen(maxlen);
        }
        args.approximateTrimming(approximateTrimming);
        return args;
    }

    @Override
    public Xadd<byte[], byte[], Map<String, Object>> operation() {
        Xadd<byte[], byte[], Map<String, Object>> operation = new Xadd<>(keyFunction(), messageFunction());
        operation.setArgs(xAddArgs());
        return operation;
    }

    private Function<Map<String, Object>, Collection<StreamMessage<byte[], byte[]>>> messageFunction() {
        Function<Map<String, Object>, byte[]> keyFunction = keyFunction();
        Function<Map<String, Object>, Map<byte[], byte[]>> mapFunction = fieldFilterArgs.mapFunction();
        return m -> Collections.singletonList(new StreamMessage<>(keyFunction.apply(m), null, mapFunction.apply(m)));
    }

    public FieldFilterArgs getFieldFilterArgs() {
        return fieldFilterArgs;
    }

    public void setFieldFilterArgs(FieldFilterArgs args) {
        this.fieldFilterArgs = args;
    }

    public long getMaxlen() {
        return maxlen;
    }

    public void setMaxlen(long maxlen) {
        this.maxlen = maxlen;
    }

    public boolean isApproximateTrimming() {
        return approximateTrimming;
    }

    public void setApproximateTrimming(boolean approximateTrimming) {
        this.approximateTrimming = approximateTrimming;
    }

}
