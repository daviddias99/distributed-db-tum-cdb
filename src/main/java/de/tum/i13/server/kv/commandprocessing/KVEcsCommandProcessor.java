package de.tum.i13.server.kv.commandprocessing;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.net.ServerCommunicator;
import de.tum.i13.server.persistentstorage.PersistentStorage;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.NetworkLocation;
import de.tum.i13.shared.hashing.ConsistentHashRing;

public class KVEcsCommandProcessor implements CommandProcessor<KVMessage> {

  private ServerState serverState;
  private PersistentStorage storage;
  private ServerCommunicator ecsCommunicator;
  private boolean asyncHandoff;

  public KVEcsCommandProcessor(PersistentStorage storage, ServerState serverState, ServerCommunicator ecsCommunicator, boolean asyncHandoff) {
    this.serverState = serverState;
    this.storage = storage;
    this.asyncHandoff = asyncHandoff;
    this.ecsCommunicator = ecsCommunicator;
  }

  public KVEcsCommandProcessor(PersistentStorage storage, ServerState serverState, ServerCommunicator ecsCommunicator) {
    this(storage, serverState, ecsCommunicator, true);
  }

  @Override
  public KVMessage process(KVMessage command, PeerType peerType) {

    if (peerType != PeerType.ECS) {
      return null;
    }

    return switch (command.getStatus()) {
      case HEART_BEAT -> new KVMessageImpl(KVMessage.StatusType.HEART_BEAT);
      case ECS_WRITE_LOCK -> this.writeLock();
      case ECS_WRITE_UNLOCK -> this.writeUnlock();
      case ECS_HANDOFF -> this.handoff(command, asyncHandoff);
      case ECS_SET_KEYRANGE -> this.setKeyRange(command);
      default -> null;
    };
  }

  private KVMessage writeLock() {
    this.serverState.writeLock();
    return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
  }

  private KVMessage writeUnlock() {
    this.serverState.start();
    return new KVMessageImpl(KVMessage.StatusType.SERVER_ACK);
  }

  public KVMessage setKeyRange(KVMessage command) {
    this.serverState.setRingMetadata(ConsistentHashRing.unpackMetadata(command.getKey()));
    this.serverState.start();
    return new KVMessageImpl(KVMessage.StatusType.SERVER_ACK);
  }

  private KVMessage handoff(KVMessage command, boolean async) {
    String[] bounds = command.getValue().split(" ");

    if (bounds.length != 2) {
      return new KVMessageImpl(KVMessage.StatusType.ERROR);
    }

    NetworkLocation peerNetworkLocation = NetworkLocation.extractNetworkLocation(command.getKey());
    Runnable handoff = new HandoffHandler(peerNetworkLocation, ecsCommunicator, bounds[0], bounds[1], storage);

    if (async) {
      Thread handoffProcess = new Thread(handoff);
      handoffProcess.start();
      return new KVMessageImpl(KVMessage.StatusType.SERVER_ACK);
    }

    return null;
  }
}
