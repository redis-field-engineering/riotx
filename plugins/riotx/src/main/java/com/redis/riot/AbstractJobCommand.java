package com.redis.riot;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.JobUtils;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public abstract class AbstractJobCommand extends AbstractCallableCommand {

    public static final String DEFAULT_JOB_REPOSITORY_NAME = "riot";

    public static final String JOB_PARAM_RUN_ID = "run.id";

    @Option(names = "--job-name", description = "Job name.", paramLabel = "<string>", hidden = true)
    private String jobName;

    @Option(names = "--repeat", description = "After the job completes keep repeating it on a fixed interval (ex 5m, 1h)", paramLabel = "<dur>")
    private RiotDuration repeatEvery;

    @ArgGroup(exclusive = false, heading = "Job options%n")
    private StepArgs stepArgs = new StepArgs();

    @ArgGroup(exclusive = false)
    private ProgressArgs progressArgs = new ProgressArgs();

    private String jobRepositoryName = DEFAULT_JOB_REPOSITORY_NAME;

    private JobRepository jobRepository;

    private PlatformTransactionManager transactionManager;

    private JobLauncher jobLauncher;

    private JobExplorer jobExplorer;

    private Random random;

    protected Runnable onJobSuccessCallback;

    @Override
    protected void initialize() {
        super.initialize();
        random = new Random();
        if (jobName == null) {
            jobName = jobName();
        }
        if (jobRepository == null) {
            try {
                jobRepository = JobUtils.jobRepositoryFactoryBean(jobRepositoryName).getObject();
            } catch (Exception e) {
                throw new RiotException("Could not create job repository", e);
            }
        }
        if (transactionManager == null) {
            transactionManager = JobUtils.resourcelessTransactionManager();
        }
        if (jobLauncher == null) {
            try {
                jobLauncher = jobLauncher();
            } catch (Exception e) {
                throw new RiotException("Could not create job launcher", e);
            }
        }
        if (jobExplorer == null) {
            try {
                jobExplorer = JobUtils.jobExplorerFactoryBean(jobRepositoryName).getObject();
            } catch (Exception e) {
                log.warn("Error getting jobExplorer", e);
                throw new RiotException("Could not create job explorer", e);
            }
        }
    }

    private JobLauncher jobLauncher() throws Exception {
        TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(new SyncTaskExecutor());
        launcher.afterPropertiesSet();
        return launcher;
    }

    private JobBuilder jobBuilder() {
        return new JobBuilder(jobName, jobRepository);
    }

    @Override
    protected void execute() {
        Job job = job();
        JobExecution jobExecution;
        try {
            jobExecution = jobLauncher.run(job, jobParameters());
        } catch (JobExecutionException e) {
            throw new RiotException(e);
        }
        if (JobUtils.isFailed(jobExecution.getExitStatus())) {
            for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                ExitStatus stepExitStatus = stepExecution.getExitStatus();
                if (JobUtils.isFailed(stepExitStatus)) {
                    if (CollectionUtils.isEmpty(stepExecution.getFailureExceptions())) {
                        throw new RiotException(stepExitStatus.getExitDescription());
                    }
                    throw wrapException(stepExecution.getFailureExceptions());
                }
            }
            throw wrapException(jobExecution.getFailureExceptions());
        }
    }

    protected JobParameters jobParameters() {
        JobParametersBuilder builder = new JobParametersBuilder();
        builder.addLong(JOB_PARAM_RUN_ID, (long) random.nextInt());
        return builder.toJobParameters();
    }

    private String jobName() {
        if (commandSpec == null) {
            return ClassUtils.getShortName(getClass());
        }
        return commandSpec.name();
    }

    private RiotException wrapException(List<Throwable> throwables) {
        if (throwables.isEmpty()) {
            return new RiotException("Job failed");
        }
        return new RiotException(throwables.get(0));
    }

    protected Job job(RiotStep<?, ?>... steps) {
        return job(Arrays.asList(steps));
    }

    protected Job job(Iterable<RiotStep<?, ?>> steps) {
        return job(flow(steps));
    }

    protected Flow flow(Iterable<RiotStep<?, ?>> steps) {
        FlowBuilder<SimpleFlow> builder = new FlowBuilder<>("sequentialStepflow");
        Iterator<RiotStep<?, ?>> iterator = steps.iterator();
        builder.start(step(iterator.next()).build());
        while (iterator.hasNext()) {
            builder.next(step(iterator.next()).build());
        }
        return builder.build();
    }

    protected Job job(Flow flow) {
        FlowJobBuilder job = jobBuilder().start(flow).build();
        job.incrementer(new RunIdIncrementer());
        job.preventRestart();
        job.listener(new JobSuccessExecutionListener(job, flow));
        return job.build();
    }

    protected <I, O> SimpleStepBuilder<I, O> step(RiotStep<I, O> step) {
        step.stepArgs(stepArgs);
        step.transactionManager(transactionManager);
        step.jobRepository(jobRepository);
        SimpleStepBuilder<I, O> builder = step.build();
        if (shouldShowProgress()) {
            ProgressStepExecutionListener listener = new ProgressStepExecutionListener();
            listener.setTaskName(step.getTaskName());
            listener.setInitialMax(step.getMaxItemCount());
            listener.setExtraMessage(step.getExtraMessage());
            listener.setProgressStyle(progressArgs.getStyle());
            listener.setUpdateInterval(progressArgs.getUpdateInterval().getValue());
            builder.listener((StepExecutionListener) listener);
            builder.listener((ItemWriteListener<?>) listener);
        }
        return builder;
    }

    private class JobSuccessExecutionListener implements JobExecutionListener {

        private final FlowJobBuilder job;

        private final Flow flow;

        private Job lastJob;

        public JobSuccessExecutionListener(FlowJobBuilder job, Flow flow) {
            this.job = job;
            this.flow = flow;
        }

        @Override
        public void afterJob(JobExecution jobExecution) {
            if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
                if (null != onJobSuccessCallback) {
                    onJobSuccessCallback.run();
                }

                if (repeatEvery == null) {
                    return;
                }

                log.info("Finished job, will run again in {}", repeatEvery);
                try {
                    Thread.sleep(repeatEvery.getValue().toMillis());
                    if (lastJob == null) {
                        lastJob = job.build();
                    }

                    Job nextJob = jobBuilder().start(flow).build().incrementer(new RunIdIncrementer()).preventRestart()
                            .listener(this).build();

                    JobParametersBuilder paramsBuilder = new JobParametersBuilder(jobExecution.getJobParameters(), jobExplorer);

                    jobLauncher.run(nextJob, paramsBuilder.addString("runTime", String.valueOf(System.currentTimeMillis()))
                            .getNextJobParameters(lastJob).toJobParameters());
                    lastJob = nextJob;
                } catch (InterruptedException | JobExecutionAlreadyRunningException | JobRestartException
                        | JobInstanceAlreadyCompleteException | JobParametersInvalidException e) {
                    throw new RiotException(e);
                }
            }
            JobExecutionListener.super.afterJob(jobExecution);
        }

    }

    protected boolean shouldShowProgress() {
        return progressArgs.getStyle() != ProgressStyle.NONE;
    }

    protected abstract Job job();

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String name) {
        this.jobName = name;
    }

    public StepArgs getJobArgs() {
        return stepArgs;
    }

    public void setJobArgs(StepArgs args) {
        this.stepArgs = args;
    }

    public String getJobRepositoryName() {
        return jobRepositoryName;
    }

    public void setJobRepositoryName(String jobRepositoryName) {
        this.jobRepositoryName = jobRepositoryName;
    }

    public JobRepository getJobRepository() {
        return jobRepository;
    }

    public void setJobRepository(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public JobLauncher getJobLauncher() {
        return jobLauncher;
    }

    public void setJobLauncher(JobLauncher jobLauncher) {
        this.jobLauncher = jobLauncher;
    }

    public JobExplorer getJobExplorer() {
        return jobExplorer;
    }

    public void setJobExplorer(JobExplorer jobExplorer) {
        this.jobExplorer = jobExplorer;
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

}
