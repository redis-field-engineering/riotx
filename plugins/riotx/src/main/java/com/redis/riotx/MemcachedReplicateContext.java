package com.redis.riotx;

import org.springframework.util.Assert;

import com.redis.riot.core.JobExecutionContext;

public class MemcachedReplicateContext extends JobExecutionContext {

	private MemcachedContext sourceMemcachedContext;
	private MemcachedContext targetMemcachedContext;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(sourceMemcachedContext, "Source Memcached context not set");
		Assert.notNull(targetMemcachedContext, "Target Memcached context not set");
		sourceMemcachedContext.afterPropertiesSet();
		targetMemcachedContext.afterPropertiesSet();
		super.afterPropertiesSet();
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (targetMemcachedContext != null) {
			targetMemcachedContext.close();
			targetMemcachedContext = null;
		}
		if (sourceMemcachedContext != null) {
			sourceMemcachedContext.close();
			sourceMemcachedContext = null;
		}
	}

	public MemcachedContext getSourceMemcachedContext() {
		return sourceMemcachedContext;
	}

	public void setSourceMemcachedContext(MemcachedContext context) {
		this.sourceMemcachedContext = context;
	}

	public MemcachedContext getTargetMemcachedContext() {
		return targetMemcachedContext;
	}

	public void setTargetMemcachedContext(MemcachedContext context) {
		this.targetMemcachedContext = context;
	}

}
