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
    this.setEcsLocation(ecsLocation);
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

  public NetworkLocation getEcsLocation() {
    return ecsLocation;
  }

  public void setEcsLocation(NetworkLocation ecsLocation) {
    this.ecsLocation = ecsLocation;
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
