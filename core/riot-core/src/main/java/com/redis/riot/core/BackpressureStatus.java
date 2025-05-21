package com.redis.riot.core;

public class BackpressureStatus {

    private final boolean apply;

    private final String reason;

    private BackpressureStatus(boolean apply, String reason) {
        this.apply = apply;
        this.reason = reason;
    }

    public static BackpressureStatus apply(String reason) {
        return new BackpressureStatus(true, reason);
    }

    public static BackpressureStatus proceed() {
        return new BackpressureStatus(false, null);
    }

    public boolean shouldApplyBackpressure() {
        return apply;
    }

    public String getReason() {
        return reason;
    }

}
