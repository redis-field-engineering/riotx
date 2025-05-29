package com.redis.spring.batch.item;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

public abstract class AbstractCountingItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> {

    private int maxItemCount = Integer.MAX_VALUE;

    protected AbstractCountingItemReader() {
        setName(ClassUtils.getShortName(getClass()));
    }

    @Override
    public void setMaxItemCount(int count) {
        super.setMaxItemCount(count);
        this.maxItemCount = count;
    }

    public int getMaxItemCount() {
        return maxItemCount;
    }

}
