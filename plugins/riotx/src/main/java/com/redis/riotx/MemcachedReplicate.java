package com.redis.riotx;

import java.util.function.Supplier;

import org.springframework.batch.core.Job;

import com.redis.riot.core.Step;
import com.redis.spring.batch.memcached.MemcachedEntry;
import com.redis.spring.batch.memcached.MemcachedItemReader;
import com.redis.spring.batch.memcached.MemcachedItemWriter;

import net.spy.memcached.MemcachedClient;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "memcached-replicate", description = "Replicate a Memcached database into another Memcached database.")
public class MemcachedReplicate extends AbstractMemcachedCommand {

	private static final String STEP_REPLICATE = "memcached-replicate-step";
	private static final String TASK_NAME = "Replicating";

	@ArgGroup(exclusive = false)
	private TargetMemcachedArgs targetMemcachedArgs = new TargetMemcachedArgs();

	private Supplier<MemcachedClient> targetClientSupplier;

	@Override
	protected void setup() {
		super.setup();
		if (targetClientSupplier == null) {
			targetClientSupplier = this::targetMemcachedClient;
		}
	}

	@Override
	protected void execute() throws Exception {
		super.execute();
		targetClientSupplier = null;
	}

	private MemcachedClient targetMemcachedClient() {
		return memcachedClient(targetMemcachedArgs.getAddresses(), targetMemcachedArgs.isTls());
	}

	@Override
	protected Job job() {
		return job(replicateStep());
	}

	private Step<MemcachedEntry, MemcachedEntry> replicateStep() {
		Step<MemcachedEntry, MemcachedEntry> step = new Step<>(reader(), writer());
		step.processor(this::process);
		step.taskName(TASK_NAME);
		step.name(STEP_REPLICATE);
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

	public TargetMemcachedArgs getTargetMemcachedArgs() {
		return targetMemcachedArgs;
	}

	public void setTargetMemcachedArgs(TargetMemcachedArgs args) {
		this.targetMemcachedArgs = args;
	}

}
