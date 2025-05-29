package com.redis.riot.core.job;

import com.redis.riot.core.RiotUtils;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;

public class StepFlowFactoryBean<T, S> implements FlowFactoryBean {

    private final RiotStep<T, S> step;

    public StepFlowFactoryBean(RiotStep<T, S> step) {
        this.step = step;
    }

    @Override
    public String getName() {
        return RiotUtils.normalizeName(step.getName() + "-flow");
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

    public RiotStep<T, S> getStep() {
        return step;
    }

}
