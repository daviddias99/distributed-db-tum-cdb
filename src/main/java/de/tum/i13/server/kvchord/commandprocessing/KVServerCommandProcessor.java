package de.tum.i13.server.kvchord.commandprocessing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessageImpl;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;

/**
 * Command processor for server KVMessages
 */
public class KVServerCommandProcessor implements CommandProcessor<KVMessage> {

  private static final Logger LOGGER = LogManager.getLogger(KVServerCommandProcessor.class);

  private PersistentStorage kvStore;

  /**
   * Create a new server KVMessage processor
   * @param storage current server storage
   */
  public KVServerCommandProcessor(PersistentStorage storage) {
      this.kvStore = storage;
  }

  @Override
  public KVMessage process(KVMessage command) {
    return switch (command.getStatus()) {
      case PUT_SERVER -> this.put(command.getKey(), command.getValue());
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
}
