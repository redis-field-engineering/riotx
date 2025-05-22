package com.redis.riot;

import com.redis.riot.core.job.FlowFactoryBean;
import com.redis.riot.core.job.JobExecutor;
import com.redis.riot.core.job.RiotStep;
import com.redis.riot.core.job.StepFlowFactoryBean;
import com.redis.spring.batch.item.AbstractCountingItemReader;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.util.function.Supplier;

@Command
public abstract class AbstractJobCommand extends AbstractCallableCommand {

    @ArgGroup(exclusive = false)
    private ProgressArgs progressArgs = new ProgressArgs();

    @ArgGroup(exclusive = false, heading = "Job options%n")
    private StepArgs stepArgs = new StepArgs();

    @CommandLine.Option(names = "--skip", description = "Number of failed items before failing the job (default: ${DEFAULT-VALUE}). Use -1 to always skip failed items.", paramLabel = "<int>")
    private int skipLimit;

    @CommandLine.Option(names = "--retry", description = "Number of times to retry failed items (default: ${DEFAULT-VALUE}). 0 and 1 both mean no retry. Use -1 to always retry.", paramLabel = "<int>")
    private int retryLimit = defaultRetryLimit();

    protected int defaultRetryLimit() {
        return 0;
    }

    private final JobExecutor jobExecutor = new JobExecutor();

    @Override
    protected void initialize() throws Exception {
        jobExecutor.afterPropertiesSet();
        super.initialize();
    }

    protected Job job(RiotStep<?, ?> step) throws Exception {
        return job(stepFlow(step));
    }

    protected <I, O> StepFlowFactoryBean<I, O> stepFlow(RiotStep<I, O> step) {
        return new StepFlowFactoryBean<>(step);
    }

    protected Job job(FlowFactoryBean flow) throws Exception {
        return jobExecutor.job(jobName(), flow);
    }

    private String jobName() {
        if (commandSpec == null) {
            return ClassUtils.getShortName(getClass());
        }
        return commandSpec.name();
    }

    protected <I, O> RiotStep<I, O> step(String name, ItemReader<I> reader, ItemWriter<O> writer) {
        RiotStep<I, O> step = new RiotStep<>();
        step.setName(name);
        step.setItemReader(reader);
        step.setItemWriter(writer);
        step.setDryRun(stepArgs.isDryRun());
        if (stepArgs.getWritesPerSecond() != null) {
            step.setWritesPerSecond(stepArgs.getWritesPerSecond().intValue());
        }
        step.setThreads(stepArgs.getThreads());
        step.setJobRepository(jobExecutor.getJobRepository());
        step.setTransactionManager(jobExecutor.getTransactionManager());
        step.setCommitInterval(stepArgs.getChunkSize());
        step.setRetryLimit(retryLimit);
        step.setSkipLimit(skipLimit);
        if (shouldShowProgress()) {
            ProgressStepExecutionListener<?> listener = new ProgressStepExecutionListener<>();
            listener.setTaskName(taskName(step));
            listener.setInitialMax(() -> itemReaderSize(step.getItemReader()));
            listener.setExtraMessage(extraMessage(step));
            listener.setProgressStyle(progressArgs.getStyle());
            listener.setUpdateInterval(progressArgs.getUpdateInterval());
            step.addListener(listener);
        }
        return step;
    }

    public long itemReaderSize(ItemReader<?> reader) {
        if (reader instanceof AbstractCountingItemReader) {
            int count = ((AbstractCountingItemReader<?>) reader).getMaxItemCount();
            if (count != Integer.MAX_VALUE) {
                return count;
            }
        }
        if (reader instanceof RedisScanItemReader) {
            return size((RedisScanItemReader<?, ?>) reader);
        }
        if (reader instanceof KeyComparisonItemReader<?, ?>) {
            return size(((KeyComparisonItemReader<?, ?>) reader).getSourceReader());
        }
        return -1;
    }

    private long size(RedisScanItemReader<?, ?> reader) {
        return BatchUtils.scanSizeEstimator(reader).getAsLong();
    }

    protected Supplier<String> extraMessage(RiotStep<?, ?> step) {
        return null;
    }

    protected abstract String taskName(RiotStep<?, ?> step);

    @Override
    protected void execute() throws Exception {
        jobExecutor.execute(job());
    }

    protected boolean shouldShowProgress() {
        return progressArgs.getStyle() != ProgressStyle.NONE;
    }

    protected abstract Job job() throws Exception;

    public StepArgs getJobArgs() {
        return stepArgs;
    }

    public void setJobArgs(StepArgs args) {
        this.stepArgs = args;
    }

    public StepArgs getStepArgs() {
        return stepArgs;
    }

    public void setStepArgs(StepArgs stepArgs) {
        this.stepArgs = stepArgs;
    }

    public ProgressArgs getProgressArgs() {
        return progressArgs;
    }

    public void setProgressArgs(ProgressArgs progressArgs) {
        this.progressArgs = progressArgs;
    }

    public JobExecutor getJobExecutor() {
        return jobExecutor;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public void setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
    }

    public int getSkipLimit() {
        return skipLimit;
    }

    public void setSkipLimit(int skipLimit) {
        this.skipLimit = skipLimit;
    }

}
