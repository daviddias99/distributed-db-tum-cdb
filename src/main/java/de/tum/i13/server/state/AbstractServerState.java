package de.tum.i13.server.state;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;


public abstract class AbstractServerState implements ServerState {
    private static final Logger LOGGER = LogManager.getLogger(AbstractServerState.class);

    private State currentState;
    private List<String> nodesToDelete = new LinkedList<>();

    protected AbstractServerState(State startState) {
        this.currentState = startState;
    }

    @Override
    public synchronized State getState() {
        return currentState;
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
    public void start () {
        this.setState(State.ACTIVE);
    }

    private synchronized void setState(State state) {
        this.currentState = state;
    }

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
    
      public synchronized List<String> getDeleteQueue() {
        return this.nodesToDelete;
      }

}
