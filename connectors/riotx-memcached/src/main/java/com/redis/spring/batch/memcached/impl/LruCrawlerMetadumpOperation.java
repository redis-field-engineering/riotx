package com.redis.spring.batch.memcached.impl;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;

public interface LruCrawlerMetadumpOperation extends Operation {

	interface Callback extends OperationCallback {
		void gotMetadump(LruMetadumpEntry entry);
	}

}