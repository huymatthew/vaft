package vaft.core;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ResettableTimer {
    private final ScheduledExecutorService executor;
    private final Duration delay;
    private final Runnable action;
    private ScheduledFuture<?> future;

    public ResettableTimer(ScheduledExecutorService executor, Duration delay, Runnable action) {
        this.executor = executor;
        this.delay = delay;
        this.action = action;
    }

    public synchronized void reset() {
        cancel();
        future = executor.schedule(action, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    public synchronized void cancel() {
        if (future != null) {
            future.cancel(false);
        }
    }
}
