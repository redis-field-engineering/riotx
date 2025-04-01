package com.redis.spring.batch.item;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ItemReader;

public interface PollableItemReader<T> extends ItemReader<T> {

	/**
	 * Tries to read a piece of input data. If such input is available within the
	 * given duration, advances to the next one otherwise returns <code>null</code>.
	 * 
	 * @param timeout how long to wait before giving up, in units of {@code unit}
	 * @param unit    a {@code TimeUnit} determining how to interpret the
	 *                {@code timeout} parameter
	 * @throws InterruptedException if interrupted while waiting
	 * @throws Exception            if a problem occurs while polling
	 * @return T the item to be processed or {@code null} if the specified waiting
	 *         time elapses before an element is available
	 */
	T poll(long timeout, TimeUnit unit) throws InterruptedException, Exception;

}
