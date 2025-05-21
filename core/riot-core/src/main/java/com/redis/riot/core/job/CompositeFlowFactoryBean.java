package com.redis.riot.core.job;

import com.redis.riot.core.RiotUtils;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CompositeFlowFactoryBean implements FlowFactoryBean {

    public enum Type {
        PARRALLEL, SEQUENTIAL
    }

    private String name;

    private Type type;

    private List<? extends FlowFactoryBean> flows;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<? extends FlowFactoryBean> getFlows() {
        return flows;
    }

    public void setFlows(List<? extends FlowFactoryBean> flows) {
        this.flows = flows;
    }

    @Override
    public Class<SimpleFlow> getObjectType() {
        return SimpleFlow.class;
    }

    @Override
    public Flow getObject() throws Exception {
        if (type == Type.SEQUENTIAL) {
            FlowBuilder<SimpleFlow> builder = new FlowBuilder<>(name);
            Iterator<? extends FlowFactoryBean> iterator = flows.iterator();
            builder.start(iterator.next().getObject());
            while (iterator.hasNext()) {
                builder.next(iterator.next().getObject());
            }
            return builder.build();
        }
        List<Flow> flowObjects = new ArrayList<>();
        for (FlowFactoryBean flow : flows) {
            flowObjects.add(flow.getObject());
        }
        ThreadPoolTaskExecutor taskExecutor = RiotUtils.threadPoolTaskExecutor(flowObjects.size());
        return new FlowBuilder<SimpleFlow>(name).split(taskExecutor).add(flowObjects.toArray(Flow[]::new)).build();
    }

}
