package com.redis.riotx;

import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;

import com.redis.riot.AbstractExportCommand;
import com.redis.riot.AbstractRedisCommand;
import com.redis.riot.AbstractRedisExportCommand;
import com.redis.riot.AbstractRedisImportCommand;
import com.redis.riot.AbstractReplicateCommand;
import com.redis.riot.RedisReaderArgs;
import com.redis.riot.Replicate;
import com.redis.riot.Replicate.CompareMode;
import com.redis.riot.ReplicateWriteLogger;
import com.redis.riot.SimpleRedisArgs;
import com.redis.riot.TargetRedisArgs;
import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.MainCommand;
import com.redis.riot.core.ProgressStyle;
import com.redis.riot.operation.OperationCommand;
import com.redis.riot.test.AbstractRiotTestBase;

import io.lettuce.core.RedisURI;
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
		private final IExecutionStrategy[] configs;

		public TestRiot(TestInfo info, IExecutionStrategy... configs) {
			this.info = info;
			this.configs = configs;
		}

		private void configure(SimpleRedisArgs redisArgs) {
			redisArgs.setUri(redisURI);
			redisArgs.setCluster(getRedisServer().isRedisCluster());
		}

		private void configure(RedisReaderArgs redisReaderArgs) {
			redisReaderArgs.setIdleTimeout(DEFAULT_IDLE_TIMEOUT_SECONDS);
			redisReaderArgs.setNotificationQueueCapacity(DEFAULT_NOTIFICATION_QUEUE_CAPACITY);
		}

		@Override
		protected IExecutionStrategy executionStrategy() {
			IExecutionStrategy strategy = super.executionStrategy();
			return r -> {
				execute(r);
				for (IExecutionStrategy config : configs) {
					config.execute(r);
				}
				return strategy.execute(r);
			};
		}

		private void configure(TargetRedisArgs args) {
			args.setCluster(getTargetRedisServer().isRedisCluster());
		}

		private int execute(ParseResult parseResult) {
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
				if (command instanceof StreamImport) {
					StreamImport streamImport = (StreamImport) command;
					configure(streamImport.getSourceRedisArgs());
					if (streamImport.getTargetRedisUri() != null) {
						streamImport.setTargetRedisUri(targetRedisURI);
						configure(streamImport.getTargetRedisArgs());
					}
				}
				if (command instanceof Replicate) {
					Replicate replicateCommand = (Replicate) command;
					replicateCommand.setCompareMode(CompareMode.NONE);
				}
			}
			return 0;
		}
	}

}
