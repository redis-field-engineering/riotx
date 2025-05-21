package com.redis.spring.batch.item.redis.common;

import io.lettuce.core.*;
import io.lettuce.core.internal.ExceptionFactory;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * Exception handling and utils to operate on.
 */
public class Exceptions {

    /**
     * Unwrap the exception if the given {@link Throwable} is a {@link ExecutionException} or {@link CompletionException}.
     *
     * @param t the root cause
     * @return the unwrapped {@link Throwable#getCause() cause} or the actual {@link Throwable}.
     */
    public static Throwable unwrap(Throwable t) {

        if (t instanceof ExecutionException || t instanceof CompletionException) {
            return t.getCause();
        }

        return t;
    }

    /**
     * Prepare an unchecked {@link RuntimeException} that will bubble upstream if thrown by an operator.
     *
     * @param t the root cause
     * @return an unchecked exception that should choose bubbling up over error callback path.
     */
    public static RuntimeException bubble(Throwable t) {

        Throwable throwableToUse = unwrap(t);

        if (throwableToUse instanceof TimeoutException) {
            return new RedisCommandTimeoutException(throwableToUse);
        }

        if (throwableToUse instanceof InterruptedException) {

            Thread.currentThread().interrupt();
            return new RedisCommandInterruptedException(throwableToUse);
        }

        if (throwableToUse instanceof RedisCommandExecutionException) {
            return createExecutionException(throwableToUse.getMessage(), throwableToUse);
        }

        if (throwableToUse instanceof RedisException) {
            return (RedisException) throwableToUse;
        }

        if (throwableToUse instanceof RuntimeException) {
            return (RuntimeException) throwableToUse;
        }

        return new RedisException(throwableToUse);
    }

    public static RedisCommandExecutionException createExecutionException(String message, Throwable cause) {

        if (message != null) {
            if (message.startsWith("OOM")) {
                return cause != null ? new RedisOOMException(message, cause) : new RedisOOMException(message);
            }
        }

        return ExceptionFactory.createExecutionException(message, cause);
    }

    /**
     * Prepare an unchecked {@link RuntimeException} that will bubble upstream for synchronization usage (i.e. on calling
     * {@link Future#get()}).
     *
     * @param t the root cause
     * @return an unchecked exception that should choose bubbling up over error callback path.
     */
    public static RuntimeException fromSynchronization(Throwable t) {

        Throwable throwableToUse = unwrap(t);

        if (throwableToUse instanceof RedisCommandTimeoutException) {
            return new RedisCommandTimeoutException(throwableToUse);
        }

        if (throwableToUse instanceof RedisCommandExecutionException) {
            return bubble(throwableToUse);
        }

        if (throwableToUse instanceof RuntimeException) {
            return new RedisException(throwableToUse);
        }

        return bubble(throwableToUse);
    }

}
