package de.tum.i13.server.persistentStorage.btree.storage;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;

public interface ChunkStorageHandler<V> {
  
  public Chunk<V> readChunkFromMemory() throws Exception;

  public void storeChunkInMemory(Chunk<V> chunk) throws StorageException;
  
  public void storeChunkInMemoryForce(Chunk<V> chunk) throws StorageException;
}
