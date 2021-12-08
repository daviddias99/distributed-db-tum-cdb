package de.tum.i13.server.state;

import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.net.NetworkLocation;

import java.util.Optional;

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
  private NetworkLocation ecsLocation;

  public ServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation) {
    this(curNetworkLocation, ecsLocation, State.STOPPED);
  }

  public ServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation, State startState) {
    this(curNetworkLocation, ecsLocation, null, startState);
  }

  public ServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation, ConsistentHashRing ringMetadata,
      State startState) {
    this.currentState = startState;
    this.setRingMetadata(ringMetadata);
    this.curNetworkLocation = curNetworkLocation;
    this.ecsLocation = ecsLocation;
  }

  public synchronized State getState() {
    return currentState;
  }

  private synchronized void setState(State state) {
    this.currentState = state;
  }

  public synchronized boolean isStopped() {
    return this.currentState == State.STOPPED;
  }

  public synchronized boolean isShutdown() {
    return this.currentState == State.SHUTDOWN;
  }

  public synchronized boolean isActive() {
    return this.currentState == State.ACTIVE;
  }

  public synchronized boolean canWrite() {
    return this.currentState != State.WRITE_LOCK;
  }

  public void stop() {
    this.setState(State.STOPPED);
  }

  public void writeLock() {
    this.setState(State.WRITE_LOCK);
  }

  public void shutdown() {
    this.setState(State.SHUTDOWN);
  }

  public void start() {
    this.setState(State.ACTIVE);
  }

  public NetworkLocation getEcsLocation() {
    return ecsLocation;
  }

  public synchronized ConsistentHashRing getRingMetadata() {
    return ringMetadata;
  }

  public synchronized void setRingMetadata(ConsistentHashRing ringMetadata) {
    this.ringMetadata = ringMetadata;
  }

  public synchronized boolean responsibleForKey(String key) {
    Optional<NetworkLocation> opt = this.ringMetadata.getResponsibleNetworkLocation(key);
    return opt
        .map(curNetworkLocation::equals)
        .orElseGet(() -> false);
  }
}
