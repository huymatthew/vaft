package vaft.dashboard;

public interface DashboardAdminServer extends AutoCloseable {
    void start();
    void close();
}
