package com.redis.spring.batch.item;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.retry.RetryPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.BatchRedisMetrics;
import com.redis.spring.batch.JobUtils;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

public abstract class AbstractAsyncItemStreamSupport<S, T> extends ItemStreamSupport {

	public static final Duration DEFAULT_POLL_DELAY = Duration.ZERO;
	public static final Duration DEFAULT_AWAIT_POLL_INTERVAL = Duration.ofMillis(1);
	public static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(3);
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final String DEFAULT_JOB_REPOSITORY_NAME = "redis";
	public static final String METRICS_PREFIX = "read.";
	public static final String JOB_PARAM_RUN_ID = "run.id";

	protected final Log log = LogFactory.getLog(getClass());
	private ItemProcessor<S, T> processor = null;

	private Duration awaitPollDelay = DEFAULT_POLL_DELAY;
	private Duration awaitPollInterval = DEFAULT_AWAIT_POLL_INTERVAL;
	private Duration awaitTimeout = DEFAULT_AWAIT_TIMEOUT;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int threads = DEFAULT_THREADS;
	private RetryPolicy retryPolicy;
	private SkipPolicy skipPolicy;
	private int skipLimit;
	private int retryLimit;
	private String jobRepositoryName = DEFAULT_JOB_REPOSITORY_NAME;
	private JobRepository jobRepository;
	private PlatformTransactionManager transactionManager = JobUtils.resourcelessTransactionManager();
	private Set<JobExecutionListener> jobExecutionListeners = new LinkedHashSet<>();
	private Set<ItemReadListener<S>> itemReadListeners = new LinkedHashSet<>();
	private Set<ItemWriteListener<T>> itemWriteListeners = new LinkedHashSet<>();
	protected MeterRegistry meterRegistry = Metrics.globalRegistry;

	private JobExecution jobExecution;
	private ItemReader<S> reader;

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		if (jobRepository == null) {
			try {
				jobRepository = JobUtils.jobRepositoryFactoryBean(jobRepositoryName).getObject();
			} catch (Exception e) {
				throw new ItemStreamException("Could not initialize job repository", e);
			}
		}
		if (jobExecution == null) {
			Job job = jobBuilder().start(step().build()).build();
			TaskExecutorJobLauncher jobLauncher;
			try {
				jobLauncher = jobLauncher();
			} catch (Exception e) {
				throw new ItemStreamException("Could not initialize job launcher", e);
			}
			try {
				jobExecution = jobLauncher.run(job, jobParameters());
			} catch (JobExecutionException e) {
				throw new ItemStreamException("Could not launch job", e);
			}
			try {
				awaitUntil(() -> jobRunning() || jobExecution.getStatus().isUnsuccessful());
			} catch (ConditionTimeoutException e) {
				List<Throwable> exceptions = jobExecution.getAllFailureExceptions();
				if (!CollectionUtils.isEmpty(exceptions)) {
					throw new ItemStreamException("Job execution unsuccessful", exceptions.get(0));
				}
			}
			if (jobExecution.getStatus().isUnsuccessful()) {
				log.warn("Execution unsuccessful for job " + job.getName() + ": " + jobExecution.getStatus());
			}
			Optional<Throwable> exception = jobExecution.getStepExecutions().stream()
					.map(StepExecution::getFailureExceptions).map(CollectionUtils::firstElement)
					.filter(Objects::nonNull).findAny();
			if (exception.isPresent()) {
				throw new ItemStreamException("Could not run job", exception.get());
			}
		}
	}

	private TaskExecutorJobLauncher jobLauncher() throws Exception {
		TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		jobLauncher.afterPropertiesSet();
		return jobLauncher;
	}

	protected JobParameters jobParameters() {
		JobParametersBuilder builder = new JobParametersBuilder();
		builder.addString(JOB_PARAM_RUN_ID, UUID.randomUUID().toString());
		return builder.toJobParameters();
	}

	private JobBuilder jobBuilder() {
		JobBuilder builder = new JobBuilder(getName(), jobRepository);
		jobExecutionListeners.forEach(builder::listener);
		return builder;
	}

	private void awaitUntil(Callable<Boolean> condition) {
		Awaitility.await().pollDelay(awaitPollDelay).pollInterval(awaitPollInterval).timeout(awaitTimeout)
				.until(condition);
	}

	protected boolean jobRunning() {
		return jobExecution.isRunning();
	}

	@Override
	public void close() throws ItemStreamException {
		if (jobExecution != null) {
			Awaitility.await().until(() -> !jobExecution.isRunning() || jobExecution.getStatus().isUnsuccessful());
			jobExecution = null;
		}
		super.close();
	}

	@SuppressWarnings("removal")
	protected SimpleStepBuilder<S, T> step() {
		SimpleStepBuilder<S, T> step = stepBuilder();
		reader = reader();
		if (threads > 1) {
			step.taskExecutor(taskExecutor());
			step.throttleLimit(threads);
			step.reader(reader);
		} else {
			step.reader(reader);
		}
		step.processor(processor);
		step.writer(writer());
		itemReadListeners.forEach(step::listener);
		itemWriteListeners.forEach(step::listener);
		FaultTolerantStepBuilder<S, T> ftStep = step.faultTolerant();
		ftStep.skip(ExecutionException.class);
		ftStep.noRetry(ExecutionException.class);
		ftStep.noSkip(TimeoutException.class);
		ftStep.retry(TimeoutException.class);
		ftStep.retryLimit(retryLimit);
		ftStep.skipLimit(skipLimit);
		ftStep.skipPolicy(skipPolicy);
		ftStep.retryPolicy(retryPolicy);
		return ftStep;
	}

	protected abstract ItemReader<S> reader();

	protected abstract ItemWriter<? super T> writer();

	private TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(threads);
		taskExecutor.setCorePoolSize(threads);
		taskExecutor.setQueueCapacity(threads);
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

	protected Tag nameTag() {
		return Tag.of("name", getName());
	}

	protected SimpleStepBuilder<S, T> stepBuilder() {
		BatchRedisMetrics.createGauge(meterRegistry, METRICS_PREFIX + "chunk", () -> chunkSize,
				"Gauge reflecting the chunk size of the reader", nameTag());
		return new StepBuilder(getName(), jobRepository).chunk(chunkSize, transactionManager);
	}

	public boolean isComplete() {
		return jobExecution == null || !jobExecution.isRunning();
	}

	public void setMeterRegistry(MeterRegistry meterRegistry) {
		this.meterRegistry = meterRegistry;
	}

	public void addJobExecutionListener(JobExecutionListener listener) {
		this.jobExecutionListeners.add(listener);
	}

	public void addItemReadListener(ItemReadListener<S> listener) {
		this.itemReadListeners.add(listener);
	}

	public void addItemWriteListener(ItemWriteListener<T> listener) {
		this.itemWriteListeners.add(listener);
	}

	public ItemReader<S> getReader() {
		return reader;
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

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
	}

	public PlatformTransactionManager getTransactionManager() {
		return transactionManager;
	}

	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	public JobExecution getJobExecution() {
		return jobExecution;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
	}

	public int getRetryLimit() {
		return retryLimit;
	}

	public void setRetryLimit(int retryLimit) {
		this.retryLimit = retryLimit;
	}

	public Duration getAwaitPollDelay() {
		return awaitPollDelay;
	}

	public void setAwaitPollDelay(Duration awaitPollDelay) {
		this.awaitPollDelay = awaitPollDelay;
	}

	public Duration getAwaitPollInterval() {
		return awaitPollInterval;
	}

	public void setAwaitPollInterval(Duration awaitPollInterval) {
		this.awaitPollInterval = awaitPollInterval;
	}

	public Duration getAwaitTimeout() {
		return awaitTimeout;
	}

	public void setAwaitTimeout(Duration awaitTimeout) {
		this.awaitTimeout = awaitTimeout;
	}

	public ItemProcessor<S, T> getProcessor() {
		return processor;
	}

	public void setProcessor(ItemProcessor<S, T> processor) {
		this.processor = processor;
	}

}
