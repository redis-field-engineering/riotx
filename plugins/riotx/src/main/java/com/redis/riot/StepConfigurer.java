package com.redis.riot;

import com.redis.riot.core.job.RiotStep;

public interface StepConfigurer {

    void configure(RiotStep<?, ?> step);

}
