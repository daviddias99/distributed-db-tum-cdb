package de.tum.i13.server.persistentStorage.btree.storage;

import java.io.IOException;
import java.io.Serializable;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;

public class ChunkMockStorageHandler<V> implements ChunkStorageHandler<V>, Serializable{

  private static final long serialVersionUID = 6529685098267757691L;

  private Chunk<V> chunk;

  @Override
  public Chunk<V> readChunkFromMemory() throws Exception {    

    return this.chunk.clone();
  }

  // TODO: change exception type;
  @Override
  public void storeChunkInMemory(Chunk<V> chunk) throws IOException {
    this.chunk = chunk;
  }

  @Override
  public void storeChunkInMemoryForce(Chunk<V> chunk) throws IOException {
    this.chunk = chunk;    
  }
}
