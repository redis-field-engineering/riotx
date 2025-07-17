package com.redis.riot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

@Command
public abstract class AbstractCallableCommand extends BaseCommand implements Callable<Integer> {

    protected Logger log;

    @Override
    public Integer call() throws Exception {
        initialize();
        try {
            execute();
        } finally {
            teardown();
        }
        return 0;
    }

    protected void initialize() throws Exception {
        if (log == null) {
            log = LoggerFactory.getLogger(getClass());
        }
    }

    protected abstract void execute() throws Exception;

    protected void teardown() {
        // do nothing
    }

    public Logger getLog() {
        return log;
    }

    public void setLog(Logger log) {
        this.log = log;
    }

}
