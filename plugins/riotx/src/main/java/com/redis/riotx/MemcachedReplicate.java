package com.redis.riotx;

import java.util.concurrent.TimeoutException;

import org.springframework.batch.core.Job;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.RiotException;
import com.redis.riot.core.Step;
import com.redis.spring.batch.memcached.MemcachedEntry;
import com.redis.spring.batch.memcached.MemcachedItemReader;
import com.redis.spring.batch.memcached.MemcachedItemWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "memcached-replicate", description = "Replicate a Memcached database into another Memcached database.")
public class MemcachedReplicate extends AbstractJobCommand<MemcachedReplicateContext> {

	private static final String STEP_NAME = "memcached-replicate-step";
	private static final String TASK_NAME = "Replicating";

	@Parameters(arity = "1", index = "0", description = "Source server address(es) int the form host:port.", paramLabel = "SOURCE")
	private InetSocketAddressList sourceAddressList;

	@Option(names = "--source-tls", description = "Establish a secure TLS connection to source server(s).")
	private boolean sourceTls;

	@Parameters(arity = "1", index = "1", description = "Target server address(es) in the form host:port.", paramLabel = "TARGET")
	private InetSocketAddressList targetAddressList;

	@Option(names = "--target-tls", description = "Establish a secure TLS connection to target server(s).")
	private boolean targetTls;

	@Override
	protected MemcachedReplicateContext newExecutionContext() {
		MemcachedContext sourceContext = new MemcachedContext();
		sourceContext.setAddresses(sourceAddressList.getAddresses());
		sourceContext.setTls(sourceTls);
		MemcachedContext targetContext = new MemcachedContext();
		targetContext.setAddresses(targetAddressList.getAddresses());
		targetContext.setTls(targetTls);
		MemcachedReplicateContext context = new MemcachedReplicateContext();
		context.setSourceMemcachedContext(sourceContext);
		context.setTargetMemcachedContext(targetContext);
		return context;
	}

	@Override
	protected Job job(MemcachedReplicateContext context) {
		MemcachedContext sourceContext = context.getSourceMemcachedContext();
		MemcachedContext targetContext = context.getTargetMemcachedContext();
		MemcachedItemReader reader = new MemcachedItemReader(sourceContext::memcachedClient);
		MemcachedItemWriter writer = new MemcachedItemWriter(targetContext::memcachedClient);
		Step<MemcachedEntry, MemcachedEntry> step = new Step<>(STEP_NAME, reader, writer);
		step.processor(this::process);
		step.taskName(TASK_NAME);
		step.skip(RiotException.class);
		step.noRetry(RiotException.class);
		step.noSkip(TimeoutException.class);
		step.retry(TimeoutException.class);
		return job(context, step);
	}

	private MemcachedEntry process(MemcachedEntry entry) {
		if (entry.getExpiration() < 0) {
			entry.setExpiration(0);
		}
		return entry;
	}

	public InetSocketAddressList getSourceAddressList() {
		return sourceAddressList;
	}

	public void setSourceAddressList(InetSocketAddressList sourceAddressList) {
		this.sourceAddressList = sourceAddressList;
	}

	public boolean isSourceTls() {
		return sourceTls;
	}

	public void setSourceTls(boolean sourceTls) {
		this.sourceTls = sourceTls;
	}

	public InetSocketAddressList getTargetAddressList() {
		return targetAddressList;
	}

	public void setTargetAddressList(InetSocketAddressList targetAddressList) {
		this.targetAddressList = targetAddressList;
	}

	public boolean isTargetTls() {
		return targetTls;
	}

	public void setTargetTls(boolean targetTls) {
		this.targetTls = targetTls;
	}

}
