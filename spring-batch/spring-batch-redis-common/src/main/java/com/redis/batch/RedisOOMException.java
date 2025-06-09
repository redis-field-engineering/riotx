package com.redis.batch;

import io.lettuce.core.RedisCommandExecutionException;

/**
 * Exception that gets thrown when Redis has run out of memory and replied with a {@code OOM} error response.
 */
@SuppressWarnings("serial")
public class RedisOOMException extends RedisCommandExecutionException {

    /**
     * Create a {@code RedisBusyException} with the specified detail message.
     *
     * @param msg the detail message.
     */
    public RedisOOMException(String msg) {
        super(msg);
    }

    /**
     * Create a {@code RedisNoScriptException} with the specified detail message and nested exception.
     *
     * @param msg the detail message.
     * @param cause the nested exception.
     */
    public RedisOOMException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
