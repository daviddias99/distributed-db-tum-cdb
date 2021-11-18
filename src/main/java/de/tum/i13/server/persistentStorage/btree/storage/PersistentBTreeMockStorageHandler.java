package de.tum.i13.server.persistentStorage.btree.storage;
import java.io.Serializable;

import de.tum.i13.server.persistentStorage.btree.PersistentBTree;
import de.tum.i13.server.persistentStorage.btree.chunk.storage.ChunkMockStorageHandler;
import de.tum.i13.server.persistentStorage.btree.chunk.storage.ChunkStorageHandler;

public class PersistentBTreeMockStorageHandler<V> implements PersistentBTreeStorageHandler<V>, Serializable {

  public PersistentBTreeMockStorageHandler() {

  }

  public void save(PersistentBTree<V> tree) {
  };

  public PersistentBTree<V> load() {
    return null;
  }

  public ChunkStorageHandler<V> createChunkStorageHandler(String chunkId) {
    return new ChunkMockStorageHandler<V>();
  }
}
