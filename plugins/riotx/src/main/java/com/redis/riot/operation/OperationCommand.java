package com.redis.riot.operation;

import java.util.Map;

import com.redis.batch.RedisBatchOperation;
import org.springframework.expression.EvaluationContext;

public interface OperationCommand {

    void setEvaluationContext(EvaluationContext context);

    RedisBatchOperation<byte[], byte[], Map<String, Object>, Object> operation();

}
