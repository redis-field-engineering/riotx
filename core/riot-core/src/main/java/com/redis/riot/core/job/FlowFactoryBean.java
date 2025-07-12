package com.redis.riot.core.job;

import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.FactoryBean;

import java.util.Arrays;
import java.util.List;

public interface FlowFactoryBean extends FactoryBean<Flow> {

    String getName();

    static CompositeFlowFactoryBean parallel(String name, FlowFactoryBean... flows) {
        return parallel(name, Arrays.asList(flows));
    }

    static CompositeFlowFactoryBean parallel(String name, List<FlowFactoryBean> flows) {
        return composite(CompositeFlowFactoryBean.Type.PARRALLEL, name, flows);
    }

    static CompositeFlowFactoryBean sequential(String name, List<FlowFactoryBean> flows) {
        return composite(CompositeFlowFactoryBean.Type.SEQUENTIAL, name, flows);
    }

    static CompositeFlowFactoryBean composite(CompositeFlowFactoryBean.Type type, String name, List<FlowFactoryBean> flows) {
        CompositeFlowFactoryBean flow = new CompositeFlowFactoryBean();
        flow.setType(type);
        flow.setName(name);
        flow.setFlows(flows);
        return flow;
    }

}
