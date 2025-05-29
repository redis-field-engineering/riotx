package com.redis.riot;

import java.util.Arrays;
import java.util.List;

import com.redis.riot.core.CompareMode;
import com.redis.riot.replicate.Replicate;
import com.redis.riot.replicate.ReplicateWriteLogger;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;

import com.redis.riot.operation.OperationCommand;

import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParseResult;

public abstract class AbstractRiotApplicationTestBase extends AbstractRiotTestBase {

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
            setDisableExceptionMessageHandling(true);
        }

        private void configure(SingleRedisArgs redisArgs) {
            redisArgs.setUri(redisURI);
            redisArgs.setCluster(getRedisServer().isRedisCluster());
        }

        @Override
        protected int executionStrategy(ParseResult parseResult) {
            for (ParseResult subParseResult : parseResult.subcommands()) {
                Object command = subParseResult.commandSpec().commandLine().getCommand();
                if (command instanceof OperationCommand) {
                    command = subParseResult.commandSpec().parent().commandLine().getCommand();
                }
                if (command instanceof AbstractJobCommand) {
                    AbstractJobCommand jobCommand = ((AbstractJobCommand) command);
                    jobCommand.getProgressArgs().setStyle(ProgressStyle.NONE);
                }
                if (command instanceof AbstractRedisCommand) {
                    configure(((AbstractRedisCommand) command).getRedisArgs());
                }
                if (command instanceof AbstractRedisExport) {
                    configure(((AbstractRedisExport) command).getRedisArgs());
                }
                if (command instanceof AbstractRedisImport) {
                    configure(((AbstractRedisImport) command).getRedisArgs());
                }
                if (command instanceof AbstractExport) {
                    ((AbstractExport) command).setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
                    ((AbstractExport) command).getReaderArgs().setEventQueueCapacity(DEFAULT_EVENT_QUEUE_CAPACITY);
                }
                if (command instanceof AbstractRedisTargetExport) {
                    AbstractRedisTargetExport targetCommand = (AbstractRedisTargetExport) command;
                    targetCommand.setSourceRedisUri(redisURI);
                    targetCommand.getSourceRedisArgs().setCluster(getRedisServer().isRedisCluster());
                    targetCommand.setTargetRedisUri(targetRedisURI);
                    configure(targetCommand.getTargetRedisArgs());
                }
                if (command instanceof AbstractTargetRedisImport) {
                    AbstractTargetRedisImport redisImport = (AbstractTargetRedisImport) command;
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
            return super.executionStrategy(parseResult);
        }

        private void configure(TargetRedisArgs args) {
            args.setCluster(getTargetRedisServer().isRedisCluster());
        }

    }

}
