package de.tum.i13.server.state;

public abstract class AbstractServerState implements ServerState {

    private State currentState;

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

}
