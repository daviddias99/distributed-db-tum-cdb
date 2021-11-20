package de.tum.i13.server.persistentStorage.btree.storage;

import java.io.Serializable;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;

/**
 * This class provides an in-memory implementation of
 * {@link ChunkStorageHandler}. Is is mainly use for testing in order
 * to abstract from disk storage issues.
 */
public class ChunkMockStorageHandler<V> implements ChunkStorageHandler<V>, Serializable{

  private static final long serialVersionUID = 6529685098267757691L;

  private Chunk<V> chunk;

  @Override
  public Chunk<V> readChunk() {    
    return this.chunk.clone();
  }

  @Override
  public void storeChunk(Chunk<V> chunk) {
    this.chunk = chunk;
  }

  @Override
  public void storeChunkForce(Chunk<V> chunk) {
    this.chunk = chunk;    
  }
}
