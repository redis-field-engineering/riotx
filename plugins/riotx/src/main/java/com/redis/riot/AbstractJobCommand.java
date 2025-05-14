package com.redis.riot;

import com.redis.riot.core.job.CompositeFlow;
import com.redis.riot.core.job.JobExecutor;
import com.redis.riot.core.job.RiotFlow;
import com.redis.riot.core.job.RiotStep;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;
import com.redis.spring.batch.item.redis.reader.RedisScanItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.ClassUtils;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.util.Arrays;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

@Command
public abstract class AbstractJobCommand extends AbstractCallableCommand {

    @ArgGroup(exclusive = false, heading = "Job options%n")
    private StepArgs stepArgs = new StepArgs();

    @ArgGroup(exclusive = false)
    private ProgressArgs progressArgs = new ProgressArgs();

    private final JobExecutor jobExecutor = new JobExecutor();

    @Override
    protected void initialize() throws Exception {
        jobExecutor.afterPropertiesSet();
        super.initialize();
    }

    protected Job job(RiotFlow flow) {
        return jobExecutor.job(jobName(), flow);
    }

    protected Job job(Iterable<RiotStep<?, ?>> steps) {
        return job(CompositeFlow.sequential("mainFlow", steps));
    }

    protected Job job(RiotStep<?, ?>... steps) {
        return job(Arrays.asList(steps));
    }

    private String jobName() {
        if (commandSpec == null) {
            return ClassUtils.getShortName(getClass());
        }
        return commandSpec.name();
    }

    protected <I, O> RiotStep<I, O> step(String name, ItemReader<I> reader, ItemWriter<O> writer) {
        RiotStep<I, O> step = new RiotStep<>(name, reader, writer);
        step.setChunkSize(stepArgs.getChunkSize());
        step.setDryRun(stepArgs.isDryRun());
        step.setRetryLimit(stepArgs.getRetryLimit());
        step.setRetryPolicy(stepArgs.getRetryPolicy());
        step.setSkipLimit(stepArgs.getSkipLimit());
        step.setSkipPolicy(stepArgs.getSkipPolicy());
        step.setSleep(stepArgs.getSleep());
        step.setThreads(stepArgs.getThreads());
        if (shouldShowProgress()) {
            ProgressStepExecutionListener<O> listener = new ProgressStepExecutionListener<>();
            listener.setTaskName(taskName(step));
            listener.setInitialMax(maxItemCount(step.getReader()));
            listener.setExtraMessage(extraMessage(step));
            listener.setProgressStyle(progressArgs.getStyle());
            listener.setUpdateInterval(progressArgs.getUpdateInterval());
            step.addExecutionListener(listener);
            step.addWriteListener(listener);
        }
        return step;
    }

    protected <I, O> Supplier<String> extraMessage(RiotStep<I, O> step) {
        return null;
    }

    protected LongSupplier maxItemCount(ItemReader<?> reader) {
        if (reader instanceof RedisScanItemReader) {
            return BatchUtils.scanSizeEstimator((RedisScanItemReader<?, ?>) reader);
        }
        if (reader instanceof KeyComparisonItemReader<?, ?>) {
            return maxItemCount(((KeyComparisonItemReader<?, ?>) reader).getSourceReader());
        }
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

}
