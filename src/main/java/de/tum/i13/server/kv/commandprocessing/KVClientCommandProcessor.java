package de.tum.i13.server.kv.commandprocessing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.persistentstorage.GetException;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;

/**
 * Command processor for client KVMessages
 */
public class KVClientCommandProcessor implements CommandProcessor<KVMessage> {

  private static final Logger LOGGER = LogManager.getLogger(KVClientCommandProcessor.class);

  private ServerState serverState;
  private PersistentStorage kvStore;

  /**
   * Create a new client KVMessage processor
   * @param storage server storage
   * @param serverState server state
   */
  public KVClientCommandProcessor(PersistentStorage storage, ServerState serverState) {
    this.kvStore = storage;
    this.serverState = serverState;
  }

  @Override
  public KVMessage process(KVMessage command) {
    return switch (command.getStatus()) {
      case PUT -> this.put(command.getKey(), command.getValue());
      case DELETE -> this.delete(command.getKey());
      case GET -> this.get(command.getKey());
      case KEYRANGE -> this.keyRange();
      default -> null;
    };
  }

  /**
   * Method to search for a key in the persistent storage.
   *
   * @param key the key to search
   * @return a KVMessage with the status of the query
   */
  private KVMessage get(String key) {
    if (!this.serverState.responsibleForKey(key)) {
      return new KVMessageImpl(this.serverState.getRingMetadata().packMessage(),
          KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
    }

    try {
      LOGGER.info("Trying to read key: {}", key);
      return kvStore.get(key);
    } catch (GetException e) {
      LOGGER.error(e);
      return new KVMessageImpl(key, KVMessage.StatusType.GET_ERROR);
    }
  }

  private KVMessage put(String key, String value) {
    if(! this.serverState.responsibleForKey(key)) {
      return new KVMessageImpl(this.serverState.getRingMetadata().packMessage(), KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
    }

    if (this.serverState.canWrite()) {
      try {
        LOGGER.info("Trying to put key: {} and value: {}", key, value);
        return kvStore.put(key, value);
      } catch (PutException e) {
        LOGGER.error(e);
        return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_ERROR);
      }
    }

    return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
  }

  private KVMessage delete(String key) {
    if(! this.serverState.responsibleForKey(key)) {
      return new KVMessageImpl(this.serverState.getRingMetadata().packMessage(), KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
    }

    if (this.serverState.canWrite()) {
      try {
        LOGGER.info("Trying to delete key: {}", key);
        return kvStore.put(key, null);
      } catch (PutException e) {
        LOGGER.error(e);
        return new KVMessageImpl(key, KVMessage.StatusType.DELETE_ERROR);
      }
    }

    return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
  }

  private KVMessage keyRange() {
    LOGGER.info("Sending key-range to client: {}", this.serverState.getRingMetadata().packMessage());

    return new KVMessageImpl(this.serverState.getRingMetadata().packMessage(), KVMessage.StatusType.KEYRANGE_SUCCESS);
  }
}
