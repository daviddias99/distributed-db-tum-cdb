package de.tum.i13.server.persistentStorage.btree.storage;
import java.io.Serializable;

import de.tum.i13.server.persistentStorage.btree.PersistentBTree;

public class PersistentBTreeMockStorageHandler<V> implements PersistentBTreeStorageHandler<V>, Serializable {

  public PersistentBTreeMockStorageHandler() {

  }

  @Override
  public void save(PersistentBTree<V> tree) {
  };

  @Override
  public PersistentBTree<V> load() {
    return null;
  }

  @Override
  public ChunkStorageHandler<V> createChunkStorageHandler(String chunkId) {
    return new ChunkMockStorageHandler<V>();
  }

  @Override
  public void delete() {
  }
}
