package de.tum.i13.server.kv.commandprocessing;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.server.kv.commandprocessing.handlers.PutDeleteReplicationHandler;
import de.tum.i13.server.state.ServerState;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.net.NetworkLocation;
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
   * 
   * @param storage     server storage
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
      case DELETE -> this.put(command.getKey(), null);
      case GET -> this.get(command.getKey());
      case KEYRANGE -> this.keyRange();
      case KEYRANGE_READ -> this.keyRangeRead();
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
    if (!this.serverState.isReadResponsibleForKey(key)) {
      return new KVMessageImpl(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
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
    synchronized (this.kvStore) {
      if (!this.serverState.isWriteResponsibleForKey(key)) {
        return new KVMessageImpl(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
      }

      if (this.serverState.canWrite()) {
        try {

          if(value == null) {
            LOGGER.info("Trying to delete key: {}", key);
          } else {
            LOGGER.info("Trying to put key: {} and value: {}", key, value);
          }

          // Put/delete item from storage
          KVMessage result = kvStore.put(key, value);

          // If successful, replicate command in successors
          if (result.getStatus() == StatusType.PUT_SUCCESS || result.getStatus() == StatusType.DELETE_SUCCESS) {

            List<NetworkLocation> readResponsible = this.serverState.getRingMetadata().getReadResponsibleNetworkLocation(key);

            // No need to replicate on self
            readResponsible.remove(this.serverState.getCurNetworkLocation());
            
            // Replicate on each successor
            for (NetworkLocation networkLocation : readResponsible) {
              (new Thread(new PutDeleteReplicationHandler(networkLocation, key, value))).start();
            }
          }

          return result;
        } catch (PutException e) {
          KVMessage error;

          if (value == null) {
            error = new KVMessageImpl(key, KVMessage.StatusType.DELETE_ERROR);
          } else {
            error = new KVMessageImpl(key, value, KVMessage.StatusType.PUT_ERROR);
          }

          return error;
        }
      }

      return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
    }

  }

  private KVMessage keyRange() {
    LOGGER.info("Sending key-range to client: {}", this.serverState.getRingMetadata().packWriteRanges());

    return new KVMessageImpl(this.serverState.getRingMetadata().packWriteRanges(), KVMessage.StatusType.KEYRANGE_SUCCESS);
  }

  private KVMessage keyRangeRead() {
    LOGGER.info("Sending reading key-range to client: {}", this.serverState.getRingMetadata().packReadRanges());

    return new KVMessageImpl(this.serverState.getRingMetadata().packWriteRanges(), KVMessage.StatusType.KEYRANGE_READ_SUCCESS);
  }
}
