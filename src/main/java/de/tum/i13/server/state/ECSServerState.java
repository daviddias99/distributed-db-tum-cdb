package de.tum.i13.server.state;

import de.tum.i13.server.kv.replication.ReplicationOrchestrator;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.net.NetworkLocation;

import java.util.List;

/**
 * Class that represents the server's state. It contains the actual state, the
 * ring metadata and the location of the ECS.
 */
public class ECSServerState extends AbstractServerState {


  private ConsistentHashRing ringMetadata;
  private final NetworkLocation curNetworkLocation;
  private final NetworkLocation ecsLocation;
  private boolean isShuttingDown;
  private ReplicationOrchestrator replicationOrchestrator;

  /**
   * Create a new server state. The server is started in a STOPPED state.
   * @param curNetworkLocation location of the current server
   * @param ecsLocation location of the ecs
   */
  public ECSServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation) {
    this(curNetworkLocation, ecsLocation, null);
  }

  /**
   * Creates a new server state, intialized with the given state.
   * @param curNetworkLocation location of the current server
   * @param ecsLocation location of the ecs
   * @param replicationOrchestrator replication orchestrator to use
   */
  public ECSServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation, ReplicationOrchestrator replicationOrchestrator) {
    this(curNetworkLocation, ecsLocation, null, State.STOPPED, replicationOrchestrator);
  }

  /**
   * Create a new server state, initialized with a given state and ring data.
   * @param curNetworkLocation location of the current server
   * @param ecsLocation location of the ecs
   * @param ringMetadata initial ring data
   * @param startState starting state
   */
  public ECSServerState(NetworkLocation curNetworkLocation, NetworkLocation ecsLocation, ConsistentHashRing ringMetadata,
                        State startState, ReplicationOrchestrator replicationOrchestrator) {
    super(startState);
    this.setRingMetadata(ringMetadata);
    this.curNetworkLocation = curNetworkLocation;
    this.ecsLocation = ecsLocation;
    this.isShuttingDown = false;
    this.replicationOrchestrator = replicationOrchestrator;
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
    return ringMetadata.isReadResponsible(curNetworkLocation, key);
  }

  @Override
  public List<NetworkLocation> getReadResponsibleNetworkLocation(String key) {
    return ringMetadata.getReadResponsibleNetworkLocation(key);
  }

  @Override
  public synchronized boolean isShutdown() {
    return this.isShuttingDown;
  }

  @Override
  public synchronized void shutdown() {
    this.isShuttingDown = false;
  }

  public synchronized NetworkLocation getCurNetworkLocation() {
    return curNetworkLocation;
  }

  @Override
  public boolean isReplicationActive() {
    return ringMetadata.isReplicationActive();
  }

  public synchronized void handleKeyRangeChange(ConsistentHashRing oldMetadata, ConsistentHashRing newMetadata) {
    if (this.replicationOrchestrator == null) {
      return;
    }

    this.replicationOrchestrator.handleKeyRangeChange(oldMetadata, newMetadata);
  }

  public synchronized void deleteReplicatedRanges() {
    if (this.replicationOrchestrator == null) {
      return;
    }

    this.replicationOrchestrator.deleteReplicatedRangesWithoutUpdate();
  }

}
