package de.tum.i13.server.state;

import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.net.NetworkLocation;

import java.util.Optional;

/**
 * Class that represents the server's state. It contains the actual state, the
 * ring metadata and the location of the ECS.
 */
public class ServerState {

  /**
   * Enum that represents the server's state
   */
  public enum State {

    /**
     * Server is active
     */
    ACTIVE,

    /**
     * Server is stopped
     */
    STOPPED,

    /**
     * Server is doing shutdown
     */
    SHUTDOWN,

    /**
     * Server is write locked
     */
    WRITE_LOCK
  }

  private State currentState;
  private ConsistentHashRing ringMetadata;
  private NetworkLocation curNetworkLocation;
  private NetworkLocation ecsLocation;

  /**
   * Create a new server state. The server is started in a STOPPED state.
   * 
   * @param curNetworkLocation location of the current server
   * @param ecsLocation        location of the ecs
   */
  public ServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation) {
    this(curNetworkLocation, ecsLocation, State.STOPPED);
  }

  /**
   * Creates a new server state, intialized with the given state.
   * 
   * @param curNetworkLocation location of the current server
   * @param ecsLocation        location of the ecs
   * @param startState         starting state
   */
  public ServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation, State startState) {
    this(curNetworkLocation, ecsLocation, null, startState);
  }

  /**
   * Create a new server state, initialized with a given state and ring data.
   * 
   * @param curNetworkLocation location of the current server
   * @param ecsLocation        location of the ecs
   * @param ringMetadata       initial ring data
   * @param startState         starting state
   */
  public ServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation, ConsistentHashRing ringMetadata,
      State startState) {
    this.currentState = startState;
    this.setRingMetadata(ringMetadata);
    this.curNetworkLocation = curNetworkLocation;
    this.ecsLocation = ecsLocation;
  }

  /**
   * Get the server's current state
   * 
   * @return current state
   */
  public synchronized State getState() {
    return currentState;
  }

  /**
   * Check if the server is stopped
   * 
   * @return true if the server is stopped
   */
  public synchronized boolean isStopped() {
    return this.currentState == State.STOPPED;
  }

  /**
   * Check if the server is on shutdown mode
   * 
   * @return true if the server is on shutdown mode
   */
  public synchronized boolean isShutdown() {
    return this.currentState == State.SHUTDOWN;
  }

  /**
   * Check if the server is active
   * 
   * @return true if the server is currently active
   */
  public synchronized boolean isActive() {
    return this.currentState == State.ACTIVE;
  }

  /**
   * Check if the server is not write locked
   * 
   * @return true if the server is not write locked
   */
  public synchronized boolean canWrite() {
    return this.currentState != State.WRITE_LOCK;
  }

  /**
   * Set the server state to stopped
   */
  public void stop() {
    this.setState(State.STOPPED);
  }

  /**
   * Write lock the server
   */
  public void writeLock() {
    this.setState(State.WRITE_LOCK);
  }

  /**
   * Set the server to shutdown mode
   */
  public void shutdown() {
    this.setState(State.SHUTDOWN);
  }

  /**
   * Set the server to active mode
   */
  public void start() {
    this.setState(State.ACTIVE);
  }

  /**
   * Get the network location of the ECS
   * 
   * @return network location of the ECS
   */
  public NetworkLocation getEcsLocation() {
    return ecsLocation;
  }

  /**
   * Get the network location of the current node
   * 
   * @return network location of the current node
   */
  public NetworkLocation getCurNetworkLocation() {
    return curNetworkLocation;
  }

  /**
   * Get the ring metadata
   * 
   * @return ring metadata
   */
  public synchronized ConsistentHashRing getRingMetadata() {
    return ringMetadata;
  }

  /**
   * Set the ring metadata
   * 
   * @param ringMetadata new ring metadata
   */
  public synchronized void setRingMetadata(ConsistentHashRing ringMetadata) {
    this.ringMetadata = ringMetadata;
  }

  /**
   * Check if a given key is the responsability of the current server (i.e.
   * writting)
   * 
   * @param key key to check
   * @return true if the server is responsible for the key
   */
  public synchronized boolean isWriteResponsibleForKey(String key) {
    return ringMetadata.isWriteResponsible(curNetworkLocation, key);
  }

  /**
   * Check if a given key is the responsability of the current server (i.e.
   * reading). This method extends {@code writeResponsibleForKey} by considering
   * responsibility through replication.
   * 
   * @param key key to check
   * @return true if the server is responsible for the key
   */
  public synchronized boolean isReadResponsibleForKey(String key) {
    return ringMetadata.isReadResponsible(curNetworkLocation, key);

  }

  private synchronized void setState(State state) {
    this.currentState = state;
  }
}
