package vaft.storage;

import java.io.IOException;

public interface PersistenceStorage {
    void saveState(PersistentState state) throws IOException;
    PersistentState loadState() throws IOException;
}
