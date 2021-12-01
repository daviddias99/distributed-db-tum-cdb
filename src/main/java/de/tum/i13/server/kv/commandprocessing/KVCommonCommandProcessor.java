package de.tum.i13.server.kv.commandprocessing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.server.persistentstorage.PersistentStorage;
import de.tum.i13.server.persistentstorage.PutException;
import de.tum.i13.shared.CommandProcessor;

public class KVCommonCommandProcessor implements CommandProcessor<KVMessage> {

  private static final Logger LOGGER = LogManager.getLogger(KVCommonCommandProcessor.class);

  private PersistentStorage kvStore;

  public KVCommonCommandProcessor(PersistentStorage kvStore) {
    this.kvStore = kvStore;
  }

  @Override
  public KVMessage process(KVMessage command, PeerType peerType) {
    return switch (command.getStatus()) {
      case PUT -> this.put(command.getKey(), command.getValue());
      case DELETE -> this.delete(command.getKey());
      default -> new KVMessageImpl(KVMessage.StatusType.ERROR);
    };
  }

  /**
   * Helper method to store a new KV pair in the database
   *
   * @param key   the key of the new pair
   * @param value the new value
   * @return a KVMessage with the status of the put operation
   */
  private KVMessage put(String key, String value) {
    try {
      LOGGER.info("Trying to put key: {} and value: {}", key, value);
      return kvStore.put(key, value);
    } catch (PutException e) {
      LOGGER.error(e);
      return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_ERROR);
    }
  }

  private KVMessage delete(String key) {
    try {
      LOGGER.info("Trying to delete key: {}", key);
      return kvStore.put(key, null);
    } catch (PutException e) {
      LOGGER.error(e);
      return new KVMessageImpl(key, KVMessage.StatusType.DELETE_ERROR);
    }
  }
}
