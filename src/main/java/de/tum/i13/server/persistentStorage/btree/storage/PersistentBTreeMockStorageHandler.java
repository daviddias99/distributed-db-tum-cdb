package de.tum.i13.server.persistentStorage.btree.storage;

import java.io.Serializable;

import de.tum.i13.server.persistentStorage.btree.PersistentBTree;

/**
 * This class provides an in-memory implementation of
 * {@link PersistentBTreeStorageHandler}. Is is mainly use for testing in order
 * to abstract from disk storage issues. This class is coupled with
 * {@link ChunkMockStorageHandler} by the use of the
 * {@code createChunkStorageHandle} method
 */
public class PersistentBTreeMockStorageHandler<V> implements PersistentBTreeStorageHandler<V>, Serializable {

  @Override
  public void save(PersistentBTree<V> tree) {
  };

  @Override
  public PersistentBTree<V> load() {
    return null;
  }

  @Override
  public ChunkMockStorageHandler<V> createChunkStorageHandler(String chunkId) {
    return new ChunkMockStorageHandler<V>();
  }

  @Override
  public void delete() {
  }
}
