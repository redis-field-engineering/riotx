package com.redis.spring.batch.item.redis.reader;

import com.redis.batch.KeyType;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;

public class KeyComparisonStat {

    private Status status;

    private KeyType type;

    private long count;

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public KeyType getType() {
        return type;
    }

    public void setType(KeyType type) {
        this.type = type;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

}
