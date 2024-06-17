package com.redis.riotx;

import java.util.function.Supplier;

import org.springframework.batch.core.Job;

import com.redis.riot.core.Step;
import com.redis.spring.batch.memcached.MemcachedEntry;
import com.redis.spring.batch.memcached.MemcachedItemReader;
import com.redis.spring.batch.memcached.MemcachedItemWriter;

import net.spy.memcached.MemcachedClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "memcached-replicate", description = "Replicate a Memcached database into another Memcached database.")
public class MemcachedReplicate extends AbstractMemcachedCommand {

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

	private Supplier<MemcachedClient> targetClientSupplier;

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		if (targetClientSupplier == null) {
			targetClientSupplier = this::targetMemcachedClient;
		}
	}

	@Override
	protected void shutdown() {
		super.shutdown();
		targetClientSupplier = null;
	}
	
	@Override
	protected MemcachedClient memcachedClient() {
		return memcachedClient(sourceAddressList.getAddresses(), sourceTls);
	}

	private MemcachedClient targetMemcachedClient() {
		return memcachedClient(targetAddressList.getAddresses(), targetTls);
	}

	@Override
	protected Job job() {
		return job(replicateStep());
	}

	private Step<MemcachedEntry, MemcachedEntry> replicateStep() {
		Step<MemcachedEntry, MemcachedEntry> step = memcachedStep(STEP_NAME, reader(), writer());
		step.processor(this::process);
		step.taskName(TASK_NAME);
		return step;
	}

	private MemcachedItemWriter writer() {
		return new MemcachedItemWriter(targetClientSupplier);
	}

	private MemcachedItemReader reader() {
		return new MemcachedItemReader(clientSupplier);
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
