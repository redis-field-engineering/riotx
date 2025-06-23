package com.redis.riot;

import com.redis.batch.KeyStatEvent;
import com.redis.riot.core.job.RiotStep;
import com.redis.spring.batch.item.redis.reader.KeyEventListenerContainer;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import io.lettuce.core.codec.StringCodec;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.unit.DataSize;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Command(name = "stats", description = "Analyze a Redis database and report statistics.")
public class Stats extends AbstractRedisCommand {

    public static final DataSize DEFAULT_BIG_THRESHOLD = DataSize.ofMegabytes(1);

    private static final String TASK_NAME = "Analyzing";

    private static final String STEP_NAME = "stats-step";

    @ArgGroup(exclusive = false)
    private RedisReaderArgs readerArgs = new RedisReaderArgs();

    @Option(names = "--mem-samples", description = "Number of sampled nested values for key memory usage.", paramLabel = "<int>")
    private int memoryUsageSamples;

    @Option(names = "--border", description = "Table border type: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private AsciiTableBorder tableBorder = StatsPrinter.DEFAULT_TABLE_BORDER;

    @Option(names = "--mem", description = "Memory usage above which a key is considered big (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    private DataSize memUsage = DEFAULT_BIG_THRESHOLD;

    @Option(names = "--byterate", description = "Write bandwidth above which a key is considered potentially problematic (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    private DataSize bandwidth = StatsPrinter.DEFAULT_WRITE_BANDWIDTH_THRESHOLD;

    @Option(names = "--keyspace", description = "Regular expression to extract the keyspace from a key (default: ${DEFAULT-VALUE}).", paramLabel = "<reg>")
    private Pattern keyspacePattern = Pattern.compile(RedisStats.DEFAULT_KEYSPACE_REGEX);

    @Option(names = "--quantiles", description = "Key size percentiles to report (default: ${DEFAULT-VALUE}).", paramLabel = "<per>")
    private short[] quantiles = StatsPrinter.DEFAULT_QUANTILES;

    @Override
    protected String taskName(RiotStep<?, ?> step) {
        return TASK_NAME;
    }

    @Override
    protected Job job() throws Exception {
        RedisScanItemReader<String, String, KeyStatEvent<String>> reader = RedisScanItemReader.stats();
        reader.setMemoryUsageSamples(memoryUsageSamples);
        configure(reader);
        RedisStats stats = new RedisStats();
        stats.setKeyspacePattern(keyspacePattern);
        stats.setBigKeyPredicate(kv -> kv.getMemoryUsage() >= memUsage.toBytes());
        KeyEventListenerContainer<String, String> listenerContainer = KeyEventListenerContainer.create(
                getRedisContext().client(), StringCodec.UTF8);
        int database = getRedisContext().uri().getDatabase();
        StatsWriter writer = new StatsWriter(stats, listenerContainer, database, readerArgs.getKeyPattern());
        RiotStep<KeyStatEvent<String>, KeyStatEvent<String>> step = step(STEP_NAME, reader, writer);
        step.addListener(new ExecutionListener(statsPrinter(stats)));
        return job(step);
    }

    @Override
    protected boolean shouldShowProgress() {
        return false;
    }

    private StatsPrinter statsPrinter(RedisStats stats) {
        StatsPrinter printer = new StatsPrinter(stats, System.out);
        printer.setWriteBandwidthThreshold(bandwidth);
        printer.setQuantiles(quantiles);
        printer.setTableBorder(tableBorder);
        return printer;
    }

    @Override
    protected void configure(RedisScanItemReader<?, ?, ?> reader) {
        log.info("Configuring reader with {}", readerArgs);
        super.configure(reader);
        readerArgs.configure(reader);
    }

    private static class StatsWriter extends AbstractItemStreamItemWriter<KeyStatEvent<String>> {

        private final RedisStats stats;

        private final KeyEventListenerContainer<String, String> listenerContainer;

        private final int database;

        private final String keyPattern;

        public StatsWriter(RedisStats stats, KeyEventListenerContainer<String, String> listenerContainer, int database,
                String keyPattern) {
            this.stats = stats;
            this.listenerContainer = listenerContainer;
            this.database = database;
            this.keyPattern = keyPattern;
        }

        @Override
        public void open(ExecutionContext executionContext) throws ItemStreamException {
            listenerContainer.receive(database, keyPattern, stats::onKeyEvent);
            listenerContainer.start();
            super.open(executionContext);
        }

        @Override
        public void close() throws ItemStreamException {
            super.close();
            listenerContainer.stop();
        }

        @Override
        public void write(Chunk<? extends KeyStatEvent<String>> chunk) throws Exception {
            chunk.forEach(stats::onStatEvent);
        }

    }

    private class ExecutionListener implements StepExecutionListener {

        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        private final StatsPrinter printer;

        private ScheduledFuture<?> scheduledTask;

        public ExecutionListener(StatsPrinter printer) {
            this.printer = printer;
        }

        @Override
        public void beforeStep(StepExecution stepExecution) {
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

    public void setReaderArgs(RedisReaderArgs readerArgs) {
        this.readerArgs = readerArgs;
    }

    public DataSize getBandwidth() {
        return bandwidth;
    }

    public void setBandwidth(DataSize bandwidth) {
        this.bandwidth = bandwidth;
    }

}
