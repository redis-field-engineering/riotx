package com.redis.riot.operation;

import java.util.Map;

import com.redis.spring.batch.item.redis.common.RedisOperation;

public interface OperationCommand {

	RedisOperation<String, String, Map<String, Object>, Object> operation();

}
