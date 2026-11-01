package vaft.concurrent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ResettableTimer implements AutoCloseable {
    private final ScheduledExecutorService executor;
    private final long minDelayMs;
    private final long maxDelayMs;
    private final Runnable action;
    private ScheduledFuture<?> future;

    public ResettableTimer(ScheduledExecutorService executor, long minDelayMs, long maxDelayMs, Runnable action) {
        this.executor = executor;
        this.minDelayMs = minDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.action = action;
    }

    /**
     * Hủy lượt đếm cũ và lập lịch chạy mới với khoảng thời gian ngẫu nhiên
     * [minDelayMs, maxDelayMs]
     */
    public synchronized void reset() {
        cancel();
        long delay = (minDelayMs == maxDelayMs)
                ? minDelayMs
                : ThreadLocalRandom.current().nextLong(minDelayMs, maxDelayMs + 1);
        future = executor.schedule(action, delay, TimeUnit.MILLISECONDS);
    }

    public synchronized void cancel() {
        if (future != null) {
            future.cancel(false);
            future = null;
        }
    }

    @Override
    public void close() {
        cancel();
    }
}
