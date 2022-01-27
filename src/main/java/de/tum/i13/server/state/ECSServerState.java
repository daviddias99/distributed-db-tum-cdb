package de.tum.i13.server.state;

import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.net.NetworkLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class that represents the server's state. It contains the actual state, the
 * ring metadata and the location of the ECS.
 */
public class ECSServerState extends AbstractServerState implements ServerState {

  private static final Logger LOGGER = LogManager.getLogger(ECSServerState.class);

  private ConsistentHashRing ringMetadata;
  private final NetworkLocation curNetworkLocation;
  private final NetworkLocation ecsLocation;

  /**
   * Create a new server state. The server is started in a STOPPED state.
   * @param curNetworkLocation location of the current server
   * @param ecsLocation location of the ecs
   */
  public ECSServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation) {
    this(curNetworkLocation, ecsLocation, State.STOPPED);
  }

  /**
   * Creates a new server state, intialized with the given state.
   * @param curNetworkLocation location of the current server
   * @param ecsLocation location of the ecs
   * @param startState starting state
   */
  public ECSServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation, State startState) {
    this(curNetworkLocation, ecsLocation, null, startState);
  }

  /**
   * Create a new server state, initialized with a given state and ring data.
   * @param curNetworkLocation location of the current server
   * @param ecsLocation location of the ecs
   * @param ringMetadata initial ring data
   * @param startState starting state
   */
  public ECSServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation, ConsistentHashRing ringMetadata,
                        State startState) {
    super(startState);
    this.setRingMetadata(ringMetadata);
    this.curNetworkLocation = curNetworkLocation;
    this.ecsLocation = ecsLocation;
  }

  /**
   * Get the network location of the ECS
   * @return network location of the ECS
   */
  public NetworkLocation getEcsLocation() {
    return ecsLocation;
  }

  /**
   * Get the ring metadata
   * @return ring metadata
   */
  public synchronized ConsistentHashRing getRingMetadata() {
    return ringMetadata;
  }

  /**
   * Set the ring metadata 
   * @param ringMetadata new ring metadata
   */
  public synchronized void setRingMetadata(ConsistentHashRing ringMetadata) {
    this.ringMetadata = ringMetadata;
  }

  @Override
  public synchronized boolean isWriteResponsible(String key) {
    return ringMetadata.isWriteResponsible(curNetworkLocation, key);
  }

  @Override
  public boolean isReadResponsible(String key) {
    // TODO Merge branch that includes replication into this branch
    LOGGER.warn("Replication is not yet implemented in this branch for the ECS servers");
    return isWriteResponsible(key);
  }

}
