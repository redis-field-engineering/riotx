package com.redis.riotx;

import java.security.GeneralSecurityException;
import java.util.concurrent.TimeoutException;

import org.springframework.batch.core.Job;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.RiotException;
import com.redis.riot.core.RiotStep;
import com.redis.spring.batch.memcached.MemcachedEntry;
import com.redis.spring.batch.memcached.MemcachedItemReader;
import com.redis.spring.batch.memcached.MemcachedItemWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "memcached-replicate", description = "Replicate a Memcached database into another Memcached database.")
public class MemcachedReplicate extends AbstractJobCommand {

	private static final String TASK_NAME = "Replicating";

	@Parameters(arity = "1", index = "0", description = "Source server address(es) int the form host:port.", paramLabel = "SOURCE")
	private InetSocketAddressList sourceAddressList;

	@Option(names = "--source-tls", description = "Establish a secure TLS connection to source server(s).")
	private boolean sourceTls;

	@Option(names = "--source-tls-host", description = "Hostname for source TLS verification.")
	private String sourceTlsHostname;

	@Option(names = "--source-insecure", description = "Skip hostname verification for source server.")
	private boolean sourceInsecure;

	@Parameters(arity = "1", index = "1", description = "Target server address(es) in the form host:port.", paramLabel = "TARGET")
	private InetSocketAddressList targetAddressList;

	@Option(names = "--target-tls", description = "Establish a secure TLS connection to target server(s).")
	private boolean targetTls;

	@Option(names = "--target-tls-host", description = "Hostname for target TLS verification.")
	private String targetTlsHostname;

	@Option(names = "--target-insecure", description = "Skip hostname verification for target server.")
	private boolean targetInsecure;

	private MemcachedContext sourceMemcachedContext;
	private MemcachedContext targetMemcachedContext;

	@Override
	protected void initialize() {
		super.initialize();
		try {
			sourceMemcachedContext = sourceMemcachedContext();
		} catch (GeneralSecurityException e) {
			throw new RiotException("Could not initialize source memcached client", e);
		}
		try {
			targetMemcachedContext = targetMemcachedContext();
		} catch (GeneralSecurityException e) {
			throw new RiotException("Could not initialized target memcached client", e);
		}
	}

	private MemcachedContext sourceMemcachedContext() throws GeneralSecurityException {
		MemcachedContext context = new MemcachedContext(sourceAddressList.getAddresses());
		context.setTls(sourceTls);
		context.setSkipTlsHostnameVerification(sourceInsecure);
		context.setHostnameForTlsVerification(sourceTlsHostname);
		return context;
	}

	private MemcachedContext targetMemcachedContext() throws GeneralSecurityException {
		MemcachedContext context = new MemcachedContext(targetAddressList.getAddresses());
		context.setTls(targetTls);
		context.setSkipTlsHostnameVerification(targetInsecure);
		context.setHostnameForTlsVerification(targetTlsHostname);
		return context;
	}

	@Override
	protected Job job() {
		MemcachedItemReader reader = new MemcachedItemReader(sourceMemcachedContext::safeMemcachedClient);
		configureAsyncStreamSupport(reader);
		MemcachedItemWriter writer = new MemcachedItemWriter(targetMemcachedContext::safeMemcachedClient);
		RiotStep<MemcachedEntry, MemcachedEntry> step = new RiotStep<>(reader, writer);
		step.processor(this::process);
		step.taskName(TASK_NAME);
		step.noSkip(TimeoutException.class);
		step.retry(TimeoutException.class);
		return job(step);
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
