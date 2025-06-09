package com.redis.riot.operation;

import java.util.Map;

import com.redis.batch.operation.Lpush;

import picocli.CommandLine.Command;

@Command(name = "lpush", description = "Insert values at the head of a list")
public class LpushCommand extends AbstractMemberOperationCommand {

	@Override
	public Lpush<String, String, Map<String, Object>> operation() {
		return new Lpush<>(keyFunction(), memberFunction());
	}

}
