package com.redis.riot.core.job;

import com.redis.riot.core.*;
import com.redis.spring.batch.JobUtils;
import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.CollectionUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class JobExecutor implements InitializingBean {

    public static final String DEFAULT_JOB_REPOSITORY_NAME = "riot";

    private static final String RUN_ID_KEY = "run.id";

    private static final int NAME_MAX_LENGTH = 80;

    private final Random random = new Random();

    private String jobRepositoryName = DEFAULT_JOB_REPOSITORY_NAME;

    private JobRepository jobRepository;

    private PlatformTransactionManager transactionManager;

    private JobLauncher jobLauncher;

    private JobExplorer jobExplorer;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (jobRepository == null) {
            jobRepository = JobUtils.jobRepositoryFactoryBean(jobRepositoryName).getObject();
        }
        if (transactionManager == null) {
            transactionManager = JobUtils.resourcelessTransactionManager();
        }
        if (jobLauncher == null) {
            jobLauncher = jobLauncher();
        }
        if (jobExplorer == null) {
            jobExplorer = JobUtils.jobExplorerFactoryBean(jobRepositoryName).getObject();
        }
    }

    private JobLauncher jobLauncher() throws Exception {
        TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(new SyncTaskExecutor());
        launcher.afterPropertiesSet();
        return launcher;
    }

    public JobExecution execute(Job job) throws JobExecutionException {
        JobExecution jobExecution = jobLauncher.run(job, jobParameters());
        if (JobUtils.isFailed(jobExecution.getExitStatus())) {
            for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                ExitStatus stepExitStatus = stepExecution.getExitStatus();
                if (JobUtils.isFailed(stepExitStatus)) {
                    if (CollectionUtils.isEmpty(stepExecution.getFailureExceptions())) {
                        throw new JobExecutionException(stepExitStatus.getExitDescription());
                    }
                    throw new JobExecutionException(
                            String.format("Could not execute job %s (id %s). Error in step %s", job.getName(),
                                    jobExecution.getJobId(), stepExecution.getStepName()),
                            stepExecution.getFailureExceptions().get(0));
                }
            }
            if (CollectionUtils.isEmpty(jobExecution.getFailureExceptions())) {
                throw new JobExecutionException(jobExecution.getExitStatus().getExitDescription());
            }
            throw new JobExecutionException(
                    String.format("Could not execute job %s (id %s)", job.getName(), jobExecution.getJobId()),
                    jobExecution.getFailureExceptions().get(0));
        }
        return jobExecution;
    }

    private JobParameters jobParameters() {
        JobParametersBuilder builder = new JobParametersBuilder();
        builder.addLong(RUN_ID_KEY, random.nextLong());
        return builder.toJobParameters();
    }

    public JobExecutor jobRepositoryName(String name) {
        this.jobRepositoryName = name;
        return this;
    }

    public JobBuilder jobBuilder(String jobName) {
        return new JobBuilder(jobName, jobRepository);
    }

    public Job job(String name, Flow flow) {
        return jobBuilder(name).start(flow).build().build();
    }

    private String normalizeStepName(String name) {
        if (name.length() > NAME_MAX_LENGTH) {
            return name.substring(0, 69) + "…" + name.substring(name.length() - 10);
        }
        return name;
    }

    public <I, O> SimpleStepBuilder<I, O> step(RiotStep<I, O> step) {
        String stepName = normalizeStepName(step.getName());
        SimpleStepBuilder<I, O> builder = new StepBuilder(stepName, jobRepository).chunk(step.getChunkSize(),
                transactionManager);
        builder.reader(reader(step));
        builder.writer(writer(step));
        builder.processor(step.getProcessor());
        builder.taskExecutor(taskExecutor(step.getThreads()));
        builder.throttleLimit(step.getThreads());
        step.getExecutionListeners().forEach(builder::listener);
        step.getWriteListeners().forEach(builder::listener);
        if (step.getReader() instanceof PollableItemReader) {
            FlushingStepBuilder<I, O> flushingStepBuilder = new FlushingStepBuilder<>(builder);
            flushingStepBuilder.flushInterval(step.getFlushInterval());
            flushingStepBuilder.idleTimeout(step.getIdleTimeout());
            builder = flushingStepBuilder;
        }
        if (step.getRetryPolicy() != RetryPolicy.NEVER || step.getSkipPolicy() != SkipPolicy.NEVER) {
            FaultTolerantStepBuilder<I, O> ftStep = builder.faultTolerant();
            step.getSkip().forEach(ftStep::skip);
            step.getNoSkip().forEach(ftStep::noSkip);
            step.getRetry().forEach(ftStep::retry);
            step.getNoRetry().forEach(ftStep::noRetry);
            ftStep.retryLimit(step.getRetryLimit());
            ftStep.retryPolicy(retryPolicy(step));
            ftStep.skipLimit(step.getSkipLimit());
            ftStep.skipPolicy(skipPolicy(step));
            builder = ftStep;
        }
        return builder;
    }

    private TaskExecutor taskExecutor(int threads) {
        if (threads == 1) {
            return new SyncTaskExecutor();
        }
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(threads);
        taskExecutor.setCorePoolSize(threads);
        taskExecutor.initialize();
        return taskExecutor;
    }

    private <I> ItemReader<I> reader(RiotStep<I, ?> step) {
        if (step.getThreads() == 1 || step.getReader() instanceof PollableItemReader) {
            return step.getReader();
        }
        if (step.getReader() instanceof ItemStreamReader) {
            SynchronizedItemStreamReader<I> synchronizedReader = new SynchronizedItemStreamReader<>();
            synchronizedReader.setDelegate((ItemStreamReader<I>) step.getReader());
            return synchronizedReader;
        }
        return new SynchronizedItemReader<>(step.getReader());
    }

    private <O> ItemWriter<O> writer(RiotStep<?, O> step) {
        ItemWriter<O> writer = step.isDryRun() ? new NoopItemWriter<>() : step.getWriter();
        if (step.getSleep() == null) {
            return writer;
        }
        return new ThrottledItemWriter<>(writer, step.getSleep());
    }

    private org.springframework.retry.RetryPolicy retryPolicy(RiotStep<?, ?> step) {
        switch (step.getRetryPolicy()) {
            case ALWAYS:
                return new AlwaysRetryPolicy();
            case NEVER:
                return new NeverRetryPolicy();
            default:
                return null;
        }
    }

    private org.springframework.batch.core.step.skip.SkipPolicy skipPolicy(RiotStep<?, ?> step) {
        switch (step.getSkipPolicy()) {
            case ALWAYS:
                return new AlwaysSkipItemSkipPolicy();
            case NEVER:
                return new NeverSkipItemSkipPolicy();
            default:
                return null;
        }
    }

    public Job job(String name, RiotFlow flow) {
        return jobBuilder(name).start(flow(flow)).build().build();
    }

    private Flow flow(RiotFlow flow) {
        if (flow instanceof StepFlow) {
            FlowBuilder<SimpleFlow> builder = new FlowBuilder<>(flow.getName());
            builder.next(step(((StepFlow) flow).getStep()).build());
            return builder.build();
        }
        if (flow instanceof CompositeFlow) {
            CompositeFlow compositeFlow = (CompositeFlow) flow;
            if (compositeFlow.getType() == CompositeFlow.Type.SEQUENTIAL) {
                FlowBuilder<SimpleFlow> builder = new FlowBuilder<>(flow.getName());
                Iterator<? extends RiotFlow> iterator = compositeFlow.getFlows().iterator();
                builder.start(flow(iterator.next()));
                while (iterator.hasNext()) {
                    builder.next(flow(iterator.next()));
                }
                return builder.build();
            }
            List<Flow> flows = compositeFlow.getFlows().stream().map(this::flow).collect(Collectors.toList());
            return new FlowBuilder<SimpleFlow>(flow.getName()).split(taskExecutor(compositeFlow.getFlows().size()))
                    .add(flows.toArray(new Flow[0])).build();
        }
        throw new IllegalArgumentException("Unsupported flow type: " + flow.getClass().getName());
    }

    public String getJobRepositoryName() {
        return jobRepositoryName;
    }

    public void setJobRepositoryName(String name) {
        this.jobRepositoryName = name;
    }

    public JobRepository getJobRepository() {
        return jobRepository;
    }

    public void setJobRepository(JobRepository repository) {
        this.jobRepository = repository;
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

}
