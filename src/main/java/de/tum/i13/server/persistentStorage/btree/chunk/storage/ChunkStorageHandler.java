package de.tum.i13.server.persistentStorage.btree.chunk.storage;

import java.io.IOException;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;

public interface ChunkStorageHandler<V> {
  
  public Chunk<V> readChunkFromMemory() throws Exception;

  // TODO: change exception type;
  public void storeChunkInMemory(Chunk<V> chunk) throws IOException;
  
  public void storeChunkInMemoryForce(Chunk<V> chunk) throws IOException;
}
