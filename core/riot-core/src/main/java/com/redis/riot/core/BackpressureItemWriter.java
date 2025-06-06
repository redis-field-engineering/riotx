package com.redis.riot.core;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.util.function.Supplier;

public class BackpressureItemWriter<T> implements ItemWriter<T> {

    private Supplier<BackpressureStatus> statusSupplier = BackpressureStatus::proceed;

    @Override
    public void write(Chunk<? extends T> chunk) throws Exception {
        BackpressureStatus status = statusSupplier.get();
        if (status.shouldApplyBackpressure()) {
            throw new BackpressureException(status.getReason());
        }
    }

    public Supplier<BackpressureStatus> getStatusSupplier() {
        return statusSupplier;
    }

    public void setStatusSupplier(Supplier<BackpressureStatus> statusSupplier) {
        this.statusSupplier = statusSupplier;
    }

}
