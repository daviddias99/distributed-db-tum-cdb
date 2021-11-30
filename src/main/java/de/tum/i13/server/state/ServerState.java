package de.tum.i13.server.state;

import de.tum.i13.shared.NetworkLocation;
import de.tum.i13.shared.hashing.ConsistentHashRing;

public class ServerState {
  public enum State {
    ACTIVE,
    STOPPED,
    SHUTDOWN,
    WRITE_LOCK
  }

  private State currentState;
  private ConsistentHashRing ringMetadata;
  private NetworkLocation curNetworkLocation;

  public ServerState(NetworkLocation curNetworkLocation) {
    this(State.STOPPED, curNetworkLocation);
  }

  public ServerState(State startState, NetworkLocation curNetworkLocation) {
    this(startState, curNetworkLocation, null);
  }

  public ServerState(State startState, NetworkLocation curNetworkLocation, ConsistentHashRing ringMetadata) {
    this.setState(startState);
    this.setRingMetadata(ringMetadata);
    this.curNetworkLocation = curNetworkLocation;
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
    return this.currentState != State.WRITE_LOCK;
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

  public ConsistentHashRing getRingMetadata() {
    return ringMetadata;
  }

  public void setRingMetadata(ConsistentHashRing ringMetadata) {
    this.ringMetadata = ringMetadata;
  }

  public boolean responsibleForKey(String key) {
    return this.ringMetadata.getResponsibleNetworkLocation(key)
        .map(curNetworkLocation::equals)
        .orElseGet(() -> false);
  }
}
