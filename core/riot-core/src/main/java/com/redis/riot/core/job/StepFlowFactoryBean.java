package com.redis.riot.core.job;

import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;

public class StepFlowFactoryBean<T, S> implements FlowFactoryBean {

    private final StepFactoryBean<T, S> step;

    public StepFlowFactoryBean(StepFactoryBean<T, S> step) {
        this.step = step;
    }

    @Override
    public String getName() {
        return step.getName() + "-flow";
    }

    @Override
    public Flow getObject() throws Exception {
        FlowBuilder<SimpleFlow> builder = new FlowBuilder<>(getName());
        builder.next(step.getObject());
        return builder.build();
    }

    @Override
    public Class<SimpleFlow> getObjectType() {
        return SimpleFlow.class;
    }

    public StepFactoryBean<T, S> getStep() {
        return step;
    }

}
