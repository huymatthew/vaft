package vaft.concurrent;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.IntSupplier;

public class ResettableTimer implements AutoCloseable {
    private final ScheduledExecutorService executor;
    private final Duration delay;
    private final Runnable action;
    private ScheduledFuture<?> future;

    public ResettableTimer(ScheduledExecutorService executor, Duration delay, Runnable action) {
        this.executor = executor; this.delay = delay; this.action = action;
    }
    public synchronized void reset() { cancel(); future = executor.schedule(action, delay.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS); }
    public synchronized void cancel() { if (future != null) future.cancel(false); }
    @Override public void close() { cancel(); }
}
