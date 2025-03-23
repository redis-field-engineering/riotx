package com.redis.riotx;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.unit.DataSize;

import com.redis.riot.AbstractRedisCommand;
import com.redis.riot.ExportStepHelper;
import com.redis.riot.MemoryUsageArgs;
import com.redis.riot.RedisReaderArgs;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "stats", description = "Analyze a Redis database and report statistics.")
public class Stats extends AbstractRedisCommand {

	public static final DataSize DEFAULT_BIG_THRESHOLD = DataSize.ofMegabytes(1);

	private static final String TASK_NAME = "Analyzing";

	@ArgGroup(exclusive = false)
	private RedisReaderArgs readerArgs = new RedisReaderArgs();

	@Option(names = "--mem-samples", description = "Number of memory usage samples for a key (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int memoryUsageSamples = MemoryUsageArgs.DEFAULT_SAMPLES;

	@Option(names = "--border", description = "Table border type: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private AsciiTableBorder tableBorder = StatsPrinter.DEFAULT_TABLE_BORDER;

	@Option(names = "--mem", description = "Memory usage above which a key is considered big (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private DataSize memUsage = DEFAULT_BIG_THRESHOLD;

	@Option(names = "--rate", description = "Write bandwidth above which a key is considered potentially problematic (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private DataSize rate = StatsPrinter.DEFAULT_WRITE_BANDWIDTH_THRESHOLD;

	@Option(names = "--keyspace", description = "Regular expression to extract the keyspace from a key (default: ${DEFAULT-VALUE}).", paramLabel = "<reg>")
	private Pattern keyspacePattern = Pattern.compile(DatabaseStats.DEFAULT_KEYSPACE_REGEX);

	@Option(names = "--quantiles", description = "Key size percentiles to report (default: ${DEFAULT-VALUE}).", paramLabel = "<per>")
	private short[] quantiles = StatsPrinter.DEFAULT_QUANTILES;

	@SuppressWarnings("rawtypes")
	@Override
	protected Job job() {
		RedisItemReader<String, String> reader = RedisItemReader.struct();
		reader.setMode(ReaderMode.LIVE);
		KeyValueRead operation = (KeyValueRead) reader.getOperation();
		operation.setMemUsageLimit(1); // lowest limit while still reading mem usage
		operation.setMemUsageSamples(memoryUsageSamples);
		configure(reader);
		DatabaseStats stats = new DatabaseStats();
		stats.setKeyspacePattern(keyspacePattern);
		stats.setBigKeyPredicate(kv -> kv.getMemoryUsage() >= memUsage.toBytes());
		reader.setKeyEventListener(stats::keyEvent);
		StatsWriter writer = new StatsWriter(stats);
		Step<KeyValue<String>, KeyValue<String>> step = new ExportStepHelper(log).step(reader, writer);
		step.taskName(TASK_NAME);
		step.executionListener(new ExecutionListener(statsPrinter(stats)));
		return job(step);
	}

	@Override
	protected boolean shouldShowProgress() {
		return false;
	}

	private StatsPrinter statsPrinter(DatabaseStats stats) {
		StatsPrinter printer = new StatsPrinter(stats, System.out);
		printer.setWriteBandwidthThreshold(rate);
		printer.setQuantiles(quantiles);
		printer.setTableBorder(tableBorder);
		return printer;
	}

	@Override
	protected void configure(RedisItemReader<?, ?> reader) {
		log.info("Configuring reader with {}", readerArgs);
		super.configure(reader);
		readerArgs.configure(reader);
	}

	private static class StatsWriter implements ItemWriter<KeyValue<String>> {

		private final DatabaseStats stats;

		public StatsWriter(DatabaseStats stats) {
			this.stats = stats;
		}

		@Override
		public void write(Chunk<? extends KeyValue<String>> chunk) throws Exception {
			chunk.forEach(stats::keyValue);
		}

	}

	private class ExecutionListener implements StepExecutionListener {

		private final StatsPrinter printer;

		private ScheduledFuture<?> scheduledTask;

		public ExecutionListener(StatsPrinter printer) {
			this.printer = printer;
		}

		@Override
		public void beforeStep(StepExecution stepExecution) {
			ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
			scheduledTask = executor.scheduleAtFixedRate(this::refresh, 0, 1, TimeUnit.SECONDS);
		}

		private void refresh() {
			try {
				printer.display();
			} catch (Exception e) {
				log.error("Could not print stats", e);
			}
		}

		@Override
		public ExitStatus afterStep(StepExecution stepExecution) {
			scheduledTask.cancel(false);
			refresh();
			return null;
		}

	}

	public RedisReaderArgs getReaderArgs() {
		return readerArgs;
	}

	public void setRedisReaderArgs(RedisReaderArgs args) {
		this.readerArgs = args;
	}

	public int getMemoryUsageSamples() {
		return memoryUsageSamples;
	}

	public void setMemoryUsageSamples(int memoryUsageSamples) {
		this.memoryUsageSamples = memoryUsageSamples;
	}

	public AsciiTableBorder getTableBorder() {
		return tableBorder;
	}

	public void setTableBorder(AsciiTableBorder tableBorder) {
		this.tableBorder = tableBorder;
	}

	public DataSize getMemUsage() {
		return memUsage;
	}

	public void setMemUsage(DataSize threshold) {
		this.memUsage = threshold;
	}

	public Pattern getKeyspacePattern() {
		return keyspacePattern;
	}

	public void setKeyspacePattern(Pattern pattern) {
		this.keyspacePattern = pattern;
	}

	public short[] getQuantiles() {
		return quantiles;
	}

	public void setQuantiles(short[] quantiles) {
		this.quantiles = quantiles;
	}

}
