package com.redis.riot.operation;

import java.util.Map;

import com.redis.batch.operation.Sadd;

import picocli.CommandLine.Command;

@Command(name = "sadd", description = "Add members to a set")
public class SaddCommand extends AbstractMemberOperationCommand {

	@Override
	public Sadd<String, String, Map<String, Object>> operation() {
		return new Sadd<>(keyFunction(), memberFunction());
	}

}
