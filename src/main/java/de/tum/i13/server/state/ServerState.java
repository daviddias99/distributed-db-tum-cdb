package de.tum.i13.server.state;

public class ServerState {
  public enum State {
    ACTIVE,
    STOPPED,
    SHUTDOWN,
    WRITE_LOCK
  }

  private State currentState;
  private String keyRangeLowerBound;
  private String keyRangeUpperBound;

  public ServerState() {
    this.setState(State.STOPPED);
  }

  public ServerState(State startState) {
    this.setState(startState);
  }

  public State getState() {
    return currentState;
  }

  public void setState(State currentState) {
    this.currentState = currentState;
  }

  public boolean isStopped() {
    return this.currentState == State.STOPPED;
  }

  public boolean isShutdown() {
    return this.currentState == State.SHUTDOWN;
  }

  public boolean isActive() {
    return this.currentState == State.ACTIVE;
  }

  public boolean canWrite() {
    return this.currentState == State.WRITE_LOCK;
  }

  public void stop() {
    this.currentState = State.STOPPED;
  }

  public void writeLock() {
    this.currentState = State.WRITE_LOCK;
  }

  public void shutdown() {
    this.currentState = State.SHUTDOWN;
  }

  public void start() {
    this.currentState = State.ACTIVE;
  }

  public void setKeyRangeUpperBound(String upperBound){
    this.keyRangeUpperBound = upperBound;
  }

  public void setKeyRangeLowerBound(String lowerBound){
    this.keyRangeLowerBound = lowerBound;
  }
}
