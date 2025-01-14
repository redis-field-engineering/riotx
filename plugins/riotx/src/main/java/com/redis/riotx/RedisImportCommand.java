package com.redis.riotx;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.Assert;

import com.redis.riot.AbstractExportCommand;
import com.redis.riot.RedisReaderArgs;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.core.processor.RegexNamedGroupFunction;
import com.redis.riot.function.KeyValueMap;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "redis-import", description = "Import data from a Redis database.")
public class RedisImportCommand extends AbstractTargetRedisImportCommand {

	private static final String TASK_NAME = "Migrating";

	@ArgGroup(exclusive = false)
	private RedisReaderArgs sourceRedisReaderArgs = new RedisReaderArgs();

	@Option(names = "--key-regex", description = "Regex for key-field extraction, e.g. '\\w+:(?<id>.+)' extracts an id field from the key", paramLabel = "<rex>")
	private Pattern keyRegex;

	protected void configureSourceRedisReader(RedisItemReader<?, ?> reader) {
		configureAsyncReader(reader);
		sourceRedisContext.configure(reader);
		log.info("Configuring {} with {}", reader.getName(), sourceRedisReaderArgs);
		sourceRedisReaderArgs.configure(reader);
	}

	@Override
	protected Job job() {
		Assert.isTrue(hasOperations(), "No Redis command specified");
		RedisItemReader<String, String> reader = reader();
		Step<KeyValue<String>, Map<String, Object>> step = new Step<>(reader, operationWriter());
		step.processor(RiotUtils.processor(mapProcessor(), processor()));
		step.taskName(TASK_NAME);
		if (reader.getMode() == ReaderMode.SCAN) {
			step.maxItemCountSupplier(reader.scanSizeEstimator());
		} else {
			AbstractExportCommand.checkNotifyConfig(reader.getClient(), log);
			log.info("Configuring export step with live true, flushInterval {}, idleTimeout {}",
					reader.getFlushInterval(), reader.getIdleTimeout());
			step.live(true);
			step.flushInterval(reader.getFlushInterval());
			step.idleTimeout(reader.getIdleTimeout());
		}
		return job(step);
	}

	private RedisItemReader<String, String> reader() {
		log.info("Creating source Redis reader with {}", sourceRedisReaderArgs);
		RedisItemReader<String, String> reader = RedisItemReader.struct();
		configureSourceRedisReader(reader);
		return reader;
	}

	protected ItemProcessor<KeyValue<String>, Map<String, Object>> mapProcessor() {
		KeyValueMap mapFunction = new KeyValueMap();
		if (keyRegex != null) {
			mapFunction.setKey(new RegexNamedGroupFunction(keyRegex));
		}
		return new FunctionItemProcessor<>(mapFunction);
	}

}
