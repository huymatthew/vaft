package vaft.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RpcExecutor implements AutoCloseable {
    private final ExecutorService executor;
    public RpcExecutor(int parallelism) { executor = Executors.newFixedThreadPool(parallelism); }
    public ExecutorService executor() { return executor; }
    @Override public void close() { executor.shutdownNow(); }
}
