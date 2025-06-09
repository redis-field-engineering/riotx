package com.redis.riot;

import com.redis.riot.core.Expression;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.TemplateExpression;
import com.redis.riot.core.function.ConsumerUnaryOperator;
import com.redis.riot.core.function.StreamItemProcessor;
import com.redis.batch.KeyValue;
import lombok.ToString;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.EvaluationContext;
import picocli.CommandLine.Option;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@ToString
public class KeyValueProcessorArgs {

    public static final boolean DEFAULT_STREAM_MESSAGE_IDS = true;

    @Option(names = "--key-proc", description = "SpEL template expression to transform key names, e.g. \"prefix:#{key}\" for 'abc' returns 'prefix:abc'.", paramLabel = "<exp>")
    private TemplateExpression keyExpression;

    @Option(names = "--type-proc", description = "SpEL expression to transform key types.", paramLabel = "<exp>")
    private Expression typeExpression;

    @Option(names = "--ttl-proc", description = "SpEL expression to transform key expiration times.", paramLabel = "<exp>")
    private Expression ttlExpression;

    @Option(names = "--no-ttl", defaultValue = "${RIOT_NO_TTL}", description = "Do not propagate key expiration times.")
    private boolean noTtl;

    @Option(names = "--stream-id", defaultValue = "${RIOT_STREAM_ID:-true}", negatable = true, fallbackValue = "true", description = "Propagate stream message IDs. Enabled by default.")
    private boolean streamMessageIds = DEFAULT_STREAM_MESSAGE_IDS;

    @Option(names = "--stream-prune", defaultValue = "${RIOT_STREAM_PRUNE}", description = "Drop empty streams.")
    private boolean prune;

    public ItemProcessor<KeyValue<String>, KeyValue<String>> processor(EvaluationContext context) {
        List<ItemProcessor<KeyValue<String>, KeyValue<String>>> processors = new ArrayList<>();
        if (keyExpression != null) {
            processors.add(processor(t -> t.setKey(keyExpression.getValue(context, t))));
        }
        if (noTtl) {
            processors.add(processor(t -> t.setTtl(null)));
        }
        if (ttlExpression != null) {
            processors.add(processor(t -> t.setTtl(Instant.ofEpochMilli(ttlExpression.getLong(context, t)))));
        }
        if (typeExpression != null) {
            processors.add(processor(t -> t.setType(typeExpression.getString(context, t))));
        }
        if (!streamMessageIds || prune) {
            StreamItemProcessor streamProcessor = new StreamItemProcessor();
            streamProcessor.setDropMessageIds(!streamMessageIds);
            streamProcessor.setPrune(prune);
            processors.add(streamProcessor);
        }
        return RiotUtils.processor(processors);
    }

    private <T> ItemProcessor<T, T> processor(Consumer<T> consumer) {
        return new FunctionItemProcessor<>(new ConsumerUnaryOperator<>(consumer));
    }

    public TemplateExpression getKeyExpression() {
        return keyExpression;
    }

    public void setKeyExpression(TemplateExpression expression) {
        this.keyExpression = expression;
    }

    public Expression getTypeExpression() {
        return typeExpression;
    }

    public void setTypeExpression(Expression expression) {
        this.typeExpression = expression;
    }

    public Expression getTtlExpression() {
        return ttlExpression;
    }

    public void setTtlExpression(Expression expression) {
        this.ttlExpression = expression;
    }

    public boolean isNoTtl() {
        return noTtl;
    }

    public void setNoTtl(boolean noTtl) {
        this.noTtl = noTtl;
    }

    public boolean isStreamMessageIds() {
        return streamMessageIds;
    }

    public void setStreamMessageIds(boolean noStreamIds) {
        this.streamMessageIds = noStreamIds;
    }

    public boolean isPrune() {
        return prune;
    }

    public void setPrune(boolean prune) {
        this.prune = prune;
    }

}
