package de.tum.i13.server.kv.commandprocessing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.commandprocessing.handlers.HandoffHandler;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.net.NetworkLocation;
import de.tum.i13.shared.hashing.ConsistentHashRing;
import de.tum.i13.shared.persistentstorage.PersistentStorage;

/**
 * Command processor for ECS KVMessages
 */
public class KVEcsCommandProcessor implements CommandProcessor<KVMessage> {
  private static final Logger LOGGER = LogManager.getLogger(KVEcsCommandProcessor.class);

  private ServerState serverState;
  private PersistentStorage storage;
  private ServerCommunicator ecsCommunicator;
  private boolean asyncHandoff;

  /**
   * Create a new ECS KVMessage processor
   * 
   * @param storage         server storage
   * @param serverState     server state
   * @param ecsCommunicator ECS communication interface
   * @param asyncHandoff    true if handoff is to be processed asynchronously,
   *                        false otherwise
   */
  public KVEcsCommandProcessor(PersistentStorage storage, ServerState serverState, ServerCommunicator ecsCommunicator,
      boolean asyncHandoff) {
    this.serverState = serverState;
    this.storage = storage;
    this.asyncHandoff = asyncHandoff;
    this.ecsCommunicator = ecsCommunicator;
  }

    /**
   * Create a new ECS KVMessage processor. This handler uses asynchronous handoffs.
   * 
   * @param storage         server storage
   * @param serverState     server state
   * @param ecsCommunicator ECS communication interface
   */
  public KVEcsCommandProcessor(PersistentStorage storage, ServerState serverState, ServerCommunicator ecsCommunicator) {
    this(storage, serverState, ecsCommunicator, true);
  }

  @Override
  public KVMessage process(KVMessage command) {
    return switch (command.getStatus()) {
      case ECS_HEART_BEAT -> this.heartBeat();
      case ECS_WRITE_LOCK -> this.writeLock();
      case ECS_WRITE_UNLOCK -> this.writeUnlock();
      case ECS_HANDOFF -> this.handoff(command);
      case ECS_SET_KEYRANGE -> this.setKeyRange(command);
      default -> null;
    };
  }

  private KVMessage heartBeat() {
    LOGGER.info("Acknowleging heartbeat");
    return new KVMessageImpl(KVMessage.StatusType.SERVER_HEART_BEAT);
  }

  private KVMessage writeLock() {
    LOGGER.info("Trying to change server state to write-lock");
    this.serverState.writeLock();
    return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
  }

  private KVMessage writeUnlock() {
    LOGGER.info("Trying to remove server write-lock");
    this.serverState.start();
    return new KVMessageImpl(KVMessage.StatusType.SERVER_ACK);
  }

  private KVMessage setKeyRange(KVMessage command) {
    LOGGER.info("Trying set server metadata");
    this.serverState.setRingMetadata(ConsistentHashRing.unpackMetadata(command.getKey()));

    if(this.serverState.isStopped()) {
      LOGGER.info("Trying to set server state to ACTIVE");
      this.serverState.start();
    }
    return new KVMessageImpl(KVMessage.StatusType.SERVER_ACK);
  }

  private KVMessage handoff(KVMessage command) {

    String[] bounds = command.getValue().split(" ");

    if (bounds.length != 2) {
      LOGGER.error("More than two values given as bounds");
      return new KVMessageImpl(KVMessage.StatusType.ERROR);
    }
    LOGGER.info("Trying to execute handoff (async={}) of [{}-{}]", asyncHandoff, bounds[0], bounds[1]);

    NetworkLocation peerNetworkLocation = NetworkLocation.extractNetworkLocation(command.getKey());
    Runnable handoff = new HandoffHandler(peerNetworkLocation, ecsCommunicator, bounds[0], bounds[1], storage);

    if (asyncHandoff) {
      Thread handoffProcess = new Thread(handoff);
      handoffProcess.start();
      LOGGER.info("Started handoff process, returing acknowlegement to ECS");
      return new KVMessageImpl(KVMessage.StatusType.SERVER_ACK);
    } else {
      handoff.run();
      LOGGER.info("Finished sync. handoff.");
    }

    return null;
  }
}