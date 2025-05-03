package com.redis.riot.core.job;

public class StepFlow implements RiotFlow {

    private String name;

    private RiotStep<?, ?> step;

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public RiotStep<?, ?> getStep() {
        return step;
    }

    public void setStep(RiotStep<?, ?> step) {
        this.step = step;
    }

    public static StepFlow of(RiotStep<?, ?> step) {
        return of(step.getName() + "Flow", step);
    }

    public static StepFlow of(String name, RiotStep<?, ?> step) {
        StepFlow flow = new StepFlow();
        flow.setName(name);
        flow.setStep(step);
        return flow;
    }

}
