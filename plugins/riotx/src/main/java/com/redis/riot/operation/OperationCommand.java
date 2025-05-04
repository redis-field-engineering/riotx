package com.redis.riot.operation;

import java.util.Map;

import com.redis.spring.batch.item.redis.common.RedisOperation;
import org.springframework.expression.EvaluationContext;

public interface OperationCommand {

    void setEvaluationContext(EvaluationContext context);

    RedisOperation<String, String, Map<String, Object>, Object> operation();

}
