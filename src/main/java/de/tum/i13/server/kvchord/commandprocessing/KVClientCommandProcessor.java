package de.tum.i13.server.kvchord.commandprocessing;

import de.tum.i13.server.cmdproc.StorageCommandProcessor;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.state.ChordServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.persistentstorage.PersistentStorage;

/**
 * Command processor for client KVMessages
 */
public class KVClientCommandProcessor implements CommandProcessor<KVMessage> {

  private final StorageCommandProcessor storageCommandProcessor;

  /**
   * Create a new client KVMessage processor
   *
   * @param storage     server storage
   * @param serverState server state
   */
  public KVClientCommandProcessor(PersistentStorage storage, ChordServerState serverState) {
    storageCommandProcessor = new StorageCommandProcessor(serverState, storage);
  }

  @Override
  public KVMessage process(KVMessage command) {
    return switch (command.getStatus()) {
      case PUT, DELETE, GET -> storageCommandProcessor.process(command);
      default -> null;
    };
  }


}
