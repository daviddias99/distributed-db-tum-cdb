package de.tum.i13.server.persistentStorage.btree.storage;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;

/**
 * Handles storage of {@link Chunk}s.
 * @param <V> Type of the values used in the BTree
 */
public interface ChunkStorageHandler<V> {
  
  public Chunk<V> readChunkFromMemory() throws StorageException;

  public void storeChunkInMemory(Chunk<V> chunk) throws StorageException;
  
  public void storeChunkInMemoryForce(Chunk<V> chunk) throws StorageException;
}
