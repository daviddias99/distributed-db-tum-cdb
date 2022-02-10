package de.tum.i13.server.state;

import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * An implementation of {@link ServerState} containing state management methods
 */
public abstract class AbstractServerState implements ServerState {

    private static final Logger LOGGER = LogManager.getLogger(AbstractServerState.class);

    private State currentState;
    private List<String> nodesToDelete = new LinkedList<>();

    /**
     * Create a new {@link AbstractServerState} with a given initial state
     *
     * @param startState initial state
     */
    protected AbstractServerState(State startState) {
        this.currentState = startState;
    }

    @Override
    public synchronized State getState() {
        return currentState;
    }

    private synchronized void setState(State state) {
        this.currentState = state;
    }

    @Override
    public synchronized boolean isStopped() {
        return this.currentState == State.STOPPED;
    }

    @Override
    public synchronized boolean isShutdown() {
        return this.currentState == State.SHUTDOWN;
    }

    @Override
    public synchronized boolean isActive() {
        return this.currentState == State.ACTIVE;
    }

    @Override
    public synchronized boolean canWrite() {
        return this.currentState != State.WRITE_LOCK;
    }

    @Override
    public synchronized void stop() {
        this.setState(State.STOPPED);
    }

    @Override
    public synchronized void writeLock() {
        this.setState(State.WRITE_LOCK);
    }

    @Override
    public synchronized void shutdown() {
        this.setState(State.SHUTDOWN);
    }

    @Override
    public void start() {
        this.setState(State.ACTIVE);
    }

    /**
     * Execute queued up deletes in given {@link PersistentStorage}
     *
     * @param storage {@link PersistentStorage} where deletes are to be executed
     */
    public void executeStoredDeletes(PersistentStorage storage) {
        for (String key : nodesToDelete) {
            try {
                LOGGER.info("Trying to delete item with key {}.", key);
                storage.put(key, null);
            } catch (PutException e) {
                LOGGER.error("Could not delete item with key {} after keyrange change.", key);
            }
        }
        nodesToDelete = new LinkedList<>();
    }

    /**
     * Get list of nodes to be deleted
     *
     * @return list of nodes to be deleted
     */
    public synchronized List<String> getDeleteQueue() {
        return this.nodesToDelete;
    }

}
