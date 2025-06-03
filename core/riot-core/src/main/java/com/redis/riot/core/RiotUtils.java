package com.redis.riot.core;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class RiotUtils {

    public static final int NAME_MAX_LENGTH = 80;

    private RiotUtils() {
    }

    public static String normalizeName(String name) {
        if (name.length() > NAME_MAX_LENGTH) {
            return name.substring(0, 69) + "…" + name.substring(name.length() - 10);
        }
        return name;
    }

    public static String mask(char[] password) {
        if (ObjectUtils.isEmpty(password)) {
            return null;
        }
        return mask(password.length);
    }

    private static String mask(int length) {
        return IntStream.range(0, length).mapToObj(i -> "*").collect(Collectors.joining());
    }

    public static String mask(String password) {
        if (ObjectUtils.isEmpty(password)) {
            return null;
        }
        return mask(password.length());
    }

    @SafeVarargs
    public static <T> ItemWriter<T> writer(ItemWriter<T>... writers) {
        CompositeItemWriter<T> composite = new CompositeItemWriter<>();
        composite.setDelegates(Arrays.asList(writers));
        return composite;
    }

    public static <S, T> ItemProcessor<S, T> processor(Collection<? extends Function<?, ?>> functions) {
        return processor(functions.toArray(new Function[0]));
    }

    public static <S, T> ItemProcessor<S, T> processor(Function<?, ?>... functions) {
        return processor(
                Stream.of(functions).filter(Objects::nonNull).map(FunctionItemProcessor::new).toArray(ItemProcessor[]::new));
    }

    public static <S, T> ItemProcessor<S, T> processor(ItemProcessor<?, ?>... processors) {
        return processor(Stream.of(processors));
    }

    public static <S, T> ItemProcessor<S, T> processor(Iterable<? extends ItemProcessor<?, ?>> processors) {
        return processor(StreamSupport.stream(processors.spliterator(), false));
    }

    @SuppressWarnings("unchecked")
    public static <S, T> ItemProcessor<S, T> processor(Stream<? extends ItemProcessor<?, ?>> processors) {
        List<? extends ItemProcessor<?, ?>> list = processors.filter(Objects::nonNull).collect(Collectors.toList());
        if (list.isEmpty()) {
            return null;
        }
        if (list.size() > 1) {
            CompositeItemProcessor<S, T> composite = new CompositeItemProcessor<>();
            composite.setDelegates(list);
            return composite;
        }
        return (ItemProcessor<S, T>) list.get(0);
    }

    public static boolean isPositive(Duration duration) {
        return duration != null && !duration.isNegative() && !duration.isZero();
    }

    public static PrintStream newPrintStream(OutputStream out) {
        return newPrintStream(out, true);
    }

    public static PrintStream newPrintStream(OutputStream out, boolean autoFlush) {
        return new PrintStream(out, autoFlush, UTF_8);
    }

    public static PrintWriter newPrintWriter(OutputStream out) {
        return newPrintWriter(out, true);
    }

    public static PrintWriter newPrintWriter(OutputStream out, boolean autoFlush) {
        return new PrintWriter(new BufferedWriter(new OutputStreamWriter(out, UTF_8)), autoFlush);
    }

    public static String toString(ByteArrayOutputStream out) {
        return out.toString(UTF_8);
    }

    public static void registerFunction(StandardEvaluationContext context, String functionName, Class<?> clazz,
            String methodName, Class<?>... parameterTypes) {
        try {
            context.registerFunction(functionName, clazz.getDeclaredMethod(methodName, parameterTypes));
        } catch (Exception e) {
            throw new UnsupportedOperationException(
                    String.format("Could not get method %s.%s", ClassUtils.getQualifiedName(clazz), methodName), e);
        }
    }

    public static <S, T> Predicate<S> predicate(Function<S, T> function, Predicate<T> predicate) {
        return s -> predicate.test(function.apply(s));
    }

    @SuppressWarnings("unchecked")
    public static <S, T> ItemWriter<S> writer(ItemProcessor<S, T> processor, ItemWriter<T> writer) {
        if (processor == null) {
            return (ItemWriter<S>) writer;
        }
        return new ProcessingItemWriter<>(processor, writer);
    }

    public static void latencyTimer(MeterRegistry registry, String name, String description, Duration duration, Tags tags) {
        Timer lag = Timer.builder(name).description(description).tags(tags).register(registry);
        lag.record(duration);
    }

    public static ThreadPoolTaskExecutor threadPoolTaskExecutor(int poolSize) {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(poolSize);
        taskExecutor.setCorePoolSize(poolSize);
        taskExecutor.initialize();
        return taskExecutor;
    }

}
