package com.redis.riot.operation;

import java.util.Map;

import com.redis.batch.operation.Sadd;

import org.springframework.expression.EvaluationContext;
import picocli.CommandLine.Command;

@Command(name = "sadd", description = "Add members to a set")
public class SaddCommand extends AbstractMemberOperationCommand {

	@Override
	public Sadd<byte[], byte[], Map<String, Object>> operation(EvaluationContext context) {
		return new Sadd<>(keyFunction(context), memberFunction(context));
	}

}
