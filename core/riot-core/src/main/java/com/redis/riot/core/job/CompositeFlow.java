package com.redis.riot.core.job;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CompositeFlow implements RiotFlow {

    public enum Type {
        PARRALLEL, SEQUENTIAL
    }

    private String name;

    private Type type;

    private Collection<? extends RiotFlow> flows;

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

    public Collection<? extends RiotFlow> getFlows() {
        return flows;
    }

    public void setFlows(Collection<? extends RiotFlow> flows) {
        this.flows = flows;
    }

    public static CompositeFlow sequential(String name, Iterable<RiotStep<?, ?>> steps) {
        CompositeFlow flow = new CompositeFlow();
        flow.setName(name);
        flow.setType(Type.SEQUENTIAL);
        flow.setFlows(StreamSupport.stream(steps.spliterator(), false).map(StepFlow::of).collect(Collectors.toList()));
        return flow;
    }

    public static CompositeFlow parrallel(String name, RiotFlow... flows) {
        return of(Type.PARRALLEL, name, flows);
    }

    public static CompositeFlow sequential(String name, RiotFlow... flows) {
        return of(Type.SEQUENTIAL, name, flows);
    }

    public static CompositeFlow of(Type type, String name, RiotFlow... flows) {
        CompositeFlow flow = new CompositeFlow();
        flow.setType(type);
        flow.setName(name);
        flow.setFlows(Arrays.asList(flows));
        return flow;
    }

}
