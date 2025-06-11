package com.redis.riot.operation;

import java.util.Map;

import com.redis.batch.operation.Rpush;

import picocli.CommandLine.Command;

@Command(name = "rpush", description = "Insert values at the tail of a list")
public class RpushCommand extends AbstractMemberOperationCommand {

	@Override
	public Rpush<byte[], byte[], Map<String, Object>> operation() {
		return new Rpush<>(keyFunction(), memberFunction());
	}

}
