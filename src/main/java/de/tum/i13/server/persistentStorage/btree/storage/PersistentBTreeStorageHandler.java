package de.tum.i13.server.persistentStorage.btree.storage;

import de.tum.i13.server.persistentStorage.btree.PersistentBTree;
import de.tum.i13.server.persistentStorage.btree.chunk.storage.ChunkStorageHandler;

public interface PersistentBTreeStorageHandler<V> {

  public void save(PersistentBTree<V> tree);

  public PersistentBTree<V> load();

  public ChunkStorageHandler<V> createChunkStorageHandler(String chunkId);

  public void delete();
}
