package com.redis.riot.core.job;

import com.redis.spring.batch.JobUtils;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.CollectionUtils;

import java.util.Random;

public class JobExecutor implements InitializingBean {

    public static final String DEFAULT_JOB_REPOSITORY_NAME = "riot";

    private static final String RUN_ID_KEY = "run.id";

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

    public Job job(String name, FlowFactoryBean flow) throws Exception {
        return jobBuilder(name).start(flow.getObject()).build().build();
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
