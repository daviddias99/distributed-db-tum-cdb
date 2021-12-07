package de.tum.i13.server.state;

import de.tum.i13.shared.NetworkLocation;
import de.tum.i13.shared.hashing.ConsistentHashRing;

import java.util.Optional;

// TODO: think about syncronization
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

  public ServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation, ConsistentHashRing ringMetadata, State startState) {
    this.setState(startState);
    this.setRingMetadata(ringMetadata);
    this.curNetworkLocation = curNetworkLocation;
  }

  public State getState() {
    return currentState;
  }

  private void setState(State currentState) {
    synchronized(this.currentState) {
      this.setState(currentState);
    }
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

  public ConsistentHashRing getRingMetadata() {
    return ringMetadata;
  }

  public NetworkLocation getEcsLocation() {
    return ecsLocation;
  }

  public void setRingMetadata(ConsistentHashRing ringMetadata) {
    this.ringMetadata = ringMetadata;
  }

  public boolean responsibleForKey(String key) {
    Optional<NetworkLocation> opt = this.ringMetadata.getResponsibleNetworkLocation(key);
    return opt
        .map(curNetworkLocation::equals)
        .orElseGet(() -> false);
  }
}
