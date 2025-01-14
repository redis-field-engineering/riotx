package com.redis.riotx;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;

import com.redis.riot.AbstractExportCommand;
import com.redis.riot.AbstractRedisCommand;
import com.redis.riot.AbstractRedisExportCommand;
import com.redis.riot.AbstractRedisImportCommand;
import com.redis.riot.AbstractReplicateCommand;
import com.redis.riot.CompareMode;
import com.redis.riot.RedisArgs;
import com.redis.riot.RedisReaderArgs;
import com.redis.riot.Replicate;
import com.redis.riot.ReplicateWriteLogger;
import com.redis.riot.TargetRedisArgs;
import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.MainCommand;
import com.redis.riot.core.ProgressStyle;
import com.redis.riot.operation.OperationCommand;
import com.redis.riot.test.AbstractRiotTestBase;

import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParseResult;

abstract class AbstractRiotxApplicationTestBase extends AbstractRiotTestBase {

	private static final String PREFIX = "riotx ";

	static {
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + ReplicateWriteLogger.class.getName(), "error");
	}

	@Override
	protected String getMainCommandPrefix() {
		return PREFIX;
	}

	@Override
	protected MainCommand mainCommand(TestInfo info, IExecutionStrategy... executionStrategies) {
		return new TestRiot(info, executionStrategies);
	}

	private class TestRiot extends Riotx {

		private final TestInfo info;
		private final List<IExecutionStrategy> configs;

		public TestRiot(TestInfo info, IExecutionStrategy... configs) {
			this.info = info;
			this.configs = Arrays.asList(configs);
		}

		private void configure(RedisArgs redisArgs) {
			redisArgs.setUri(redisURI);
			redisArgs.setCluster(getRedisServer().isRedisCluster());
		}

		private void configure(RedisReaderArgs redisReaderArgs) {
			redisReaderArgs.setIdleTimeout(DEFAULT_IDLE_TIMEOUT_SECONDS);
			redisReaderArgs.setEventQueueCapacity(DEFAULT_EVENT_QUEUE_CAPACITY);
		}

		@Override
		protected int execute(ParseResult parseResult, IExecutionStrategy defaultStrategy) {
			for (ParseResult subParseResult : parseResult.subcommands()) {
				Object command = subParseResult.commandSpec().commandLine().getCommand();
				if (command instanceof OperationCommand) {
					command = subParseResult.commandSpec().parent().commandLine().getCommand();
				}
				if (command instanceof AbstractJobCommand) {
					AbstractJobCommand jobCommand = ((AbstractJobCommand) command);
					jobCommand.getJobArgs().getProgressArgs().setStyle(ProgressStyle.NONE);
					jobCommand.setJobName(name(info));
				}
				if (command instanceof AbstractRedisCommand) {
					configure(((AbstractRedisCommand) command).getRedisArgs());
				}
				if (command instanceof AbstractRedisExportCommand) {
					configure(((AbstractRedisExportCommand) command).getRedisArgs());
				}
				if (command instanceof AbstractRedisImportCommand) {
					configure(((AbstractRedisImportCommand) command).getRedisArgs());
				}
				if (command instanceof AbstractExportCommand) {
					configure(((AbstractExportCommand) command).getSourceRedisReaderArgs());
				}
				if (command instanceof AbstractReplicateCommand) {
					AbstractReplicateCommand targetCommand = (AbstractReplicateCommand) command;
					configure(targetCommand.getSourceRedisReaderArgs());
					targetCommand.setSourceRedisUri(redisURI);
					targetCommand.getSourceRedisArgs().setCluster(getRedisServer().isRedisCluster());
					targetCommand.setTargetRedisUri(targetRedisURI);
					configure(targetCommand.getTargetRedisArgs());
				}
				if (command instanceof AbstractTargetRedisImportCommand) {
					AbstractTargetRedisImportCommand redisImport = (AbstractTargetRedisImportCommand) command;
					configure(redisImport.getSourceRedisArgs());
					if (redisImport.getTargetRedisUri() != null) {
						redisImport.setTargetRedisUri(targetRedisURI);
						configure(redisImport.getTargetRedisArgs());
					}
				}
				if (command instanceof Replicate) {
					Replicate replicateCommand = (Replicate) command;
					replicateCommand.setCompareMode(CompareMode.NONE);
				}
			}
			configs.forEach(c -> c.execute(parseResult));
			return super.execute(parseResult, defaultStrategy);
		}

		private void configure(TargetRedisArgs args) {
			args.setCluster(getTargetRedisServer().isRedisCluster());
		}

	}

}
