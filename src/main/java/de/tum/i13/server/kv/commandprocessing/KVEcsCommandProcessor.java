package de.tum.i13.server.kv.commandprocessing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;

public class KVEcsCommandProcessor implements CommandProcessor<KVMessage> {


  private static final Logger LOGGER = LogManager.getLogger(KVEcsCommandProcessor.class);

  private ServerState serverState;

  public KVEcsCommandProcessor(ServerState serverState) {
    this.serverState = serverState;
  }


  @Override
  public KVMessage process(KVMessage command, PeerType peerType) {

    if (peerType != PeerType.ECS) {
      return null;
    }

    return switch (command.getStatus()) {
      case HEART_BEAT -> new KVMessageImpl(KVMessage.StatusType.HEART_BEAT);
      case ECS_WRITE_LOCK -> this.writeLock();
      case ECS_KEYRANGE -> this.writeLock();
      default -> null;
    };
  }

  private KVMessage writeLock() {
    this.serverState.writeLock();
    return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
}
}
