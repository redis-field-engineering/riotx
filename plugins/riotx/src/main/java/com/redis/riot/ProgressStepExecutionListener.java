package com.redis.riot;

import java.time.Duration;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.Chunk;

import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

/**
 * Listener tracking writer or step progress with by a progress bar.
 *
 * @author Julien Ruaux
 * @since 3.1.2
 */
public class ProgressStepExecutionListener<S> implements StepExecutionListener, ItemWriteListener<S> {

    private String taskName;

    private Duration updateInterval;

    private LongSupplier initialMax;

    private ProgressStyle progressStyle = ProgressStyle.ASCII;

    private Supplier<String> extraMessage;

    private ProgressBar progressBar;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        ProgressBarBuilder progressBarBuilder = new ProgressBarBuilder();
        if (taskName != null) {
            progressBarBuilder.setTaskName(taskName);
        }
        progressBarBuilder.setStyle(progressBarStyle());
        if (updateInterval != null) {
            progressBarBuilder.setUpdateIntervalMillis(Math.toIntExact(updateInterval.toMillis()));
        }
        progressBarBuilder.showSpeed();
        if (progressStyle == ProgressStyle.LOG) {
            Logger logger = LoggerFactory.getLogger(getClass());
            progressBarBuilder.setConsumer(new DelegatingProgressBarConsumer(logger::info));
        }
        if (initialMax != null) {
            progressBarBuilder.setInitialMax(initialMax.getAsLong());
        }
        this.progressBar = progressBarBuilder.build();
    }

    private ProgressBarStyle progressBarStyle() {
        switch (progressStyle) {
            case BAR:
                return ProgressBarStyle.COLORFUL_UNICODE_BAR;
            case BLOCK:
                return ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
            default:
                return ProgressBarStyle.ASCII;
        }
    }

    @Override
    public void afterWrite(Chunk<? extends S> items) {
        if (progressBar != null) {
            progressBar.stepBy(items.size());
            if (extraMessage != null) {
                progressBar.setExtraMessage(extraMessage.get());
            }
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        if (progressBar != null) {
            if (!stepExecution.getStatus().isUnsuccessful()) {
                progressBar.stepTo(progressBar.getMax());
            }
            progressBar.close();
            progressBar = null;
        }
        return stepExecution.getExitStatus();
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public LongSupplier getInitialMax() {
        return initialMax;
    }

    public void setInitialMax(LongSupplier initialMax) {
        this.initialMax = initialMax;
    }

    public Supplier<String> getExtraMessage() {
        return extraMessage;
    }

    public void setExtraMessage(Supplier<String> extraMessage) {
        this.extraMessage = extraMessage;
    }

    public Duration getUpdateInterval() {
        return updateInterval;
    }

    public void setUpdateInterval(Duration updateInterval) {
        this.updateInterval = updateInterval;
    }

    public ProgressStyle getProgressStyle() {
        return progressStyle;
    }

    public void setProgressStyle(ProgressStyle progressStyle) {
        this.progressStyle = progressStyle;
    }

}
