package de.tum.i13.server.persistentStorage.btree.storage;

import java.io.Serializable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.persistentStorage.btree.PersistentBTree;

/**
 * This class provides an in-memory implementation of
 * {@link PersistentBTreeStorageHandler}. Is is mainly use for testing in order
 * to abstract from disk storage issues. This class is coupled with
 * {@link ChunkMockStorageHandler} by the use of the
 * {@code createChunkStorageHandle} method
 */
public class PersistentBTreeMockStorageHandler<V> implements PersistentBTreeStorageHandler<V>, Serializable {
  private static final Logger LOGGER = LogManager.getLogger(PersistentBTreeMockStorageHandler.class);

  PersistentBTree<V> tree;

  @Override
  public void save(PersistentBTree<V> tree) {
    LOGGER.info("Saved tree ({}) from memory.", tree.hashCode());
    this.tree = tree;
  };

  @Override
  public PersistentBTree<V> load() {
    LOGGER.info("Loaded tree ({}) from memory.", tree.hashCode());
    return this.tree;
  }

  @Override
  public ChunkMockStorageHandler<V> createChunkStorageHandler(String chunkId) {
    return new ChunkMockStorageHandler<V>();
  }

  @Override
  public void delete() {
    LOGGER.info("Deleted tree ({}) from memory.", tree.hashCode());
    this.tree = null;
  }
}
