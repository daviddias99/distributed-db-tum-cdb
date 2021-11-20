package de.tum.i13.server.persistentStorage.btree.storage;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;

/**
 * Handles storage of {@link Chunk}s.
 * 
 * @param <V> Type of the values used in the BTree.
 */
public interface ChunkStorageHandler<V> {

  /**
   * Read the stored chunk.
   * 
   * @return The stored chunk
   * @throws StorageException An exception is thrown when the handler wansn't able
   *                          to read the chunk
   */
  public Chunk<V> readChunk() throws StorageException;

  /**
   * Store the chunk. If the chunk is empty, the chunk is deleted instead.
   * 
   * @return The stored chunk
   * @throws StorageException An exception is thrown when the handler wansn't able
   *                          to store or delete the chunk
   */
  public void storeChunk(Chunk<V> chunk) throws StorageException;

  /**
   * Store the chunk. This method differs from {@link #storeChunk(Chunk)
   * storeChunk} by storing the chunk even if it's empty.
   * 
   * @return The stored chunk
   * @throws StorageException An exception is thrown when the handler wansn't able
   *                          to store or delete the chunk
   */
  public void storeChunkForce(Chunk<V> chunk) throws StorageException;
}
