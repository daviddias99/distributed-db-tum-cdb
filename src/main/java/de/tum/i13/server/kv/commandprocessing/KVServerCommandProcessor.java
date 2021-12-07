package de.tum.i13.server.kv.commandprocessing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.server.kv.PeerAuthenticator.PeerType;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;

public class KVServerCommandProcessor implements CommandProcessor<KVMessage> {

  private static final Logger LOGGER = LogManager.getLogger(KVServerCommandProcessor.class);

  private PersistentStorage kvStore;

  public KVServerCommandProcessor(PersistentStorage storage) {
      this.kvStore = storage;
  }

  @Override
  public KVMessage process(KVMessage command, PeerType peerType) {
    if (peerType != PeerType.SERVER) {
      return null;
    }

    return switch (command.getStatus()) {
      case PUT -> this.put(command.getKey(), command.getValue());
      case DELETE -> this.delete(command.getKey());
      default -> null;
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
