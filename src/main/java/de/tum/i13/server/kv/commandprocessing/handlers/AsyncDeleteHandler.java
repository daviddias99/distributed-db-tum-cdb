package de.tum.i13.server.kv.commandprocessing.handlers;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.shared.persistentstorage.PersistentStorage;
import de.tum.i13.shared.persistentstorage.PutException;

public class AsyncDeleteHandler implements Runnable{
  private static final Logger LOGGER = LogManager.getLogger(AsyncDeleteHandler.class);

  private final List<String> keys;
  private final PersistentStorage storage;

  public AsyncDeleteHandler(PersistentStorage storage, List<String> keys) {
    this.keys = keys;
    this.storage = storage;
  }

  @Override
  public void run() {
    LOGGER.info("Deleting {} replicated keys from self.", keys.size());

    for (String key : keys) {
      try {
        storage.put(key, null);
      } catch (PutException e) {
        LOGGER.error("Could not delete item with key {}.", key);
      }
    }
  } 
}
