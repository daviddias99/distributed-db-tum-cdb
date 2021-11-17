package de.tum.i13.server.persistentStorage.btree.storage;

import de.tum.i13.server.persistentStorage.btree.PersistentBTree;
import de.tum.i13.server.persistentStorage.btree.chunk.storage.ChunkStorageHandler;

public interface PersistentBTreeStorageHandler<V> {

  public void saveToDisk(PersistentBTree<V> tree);

  public PersistentBTree<V> readFromDisk();

  public ChunkStorageHandler<V> createChunkStorageHandler(String chunkId);
}
