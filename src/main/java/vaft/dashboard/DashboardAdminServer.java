package vaft.dashboard;

public interface DashboardAdminServer extends AutoCloseable {
    void start();
    @Override void close();
}
