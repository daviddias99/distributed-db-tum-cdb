package de.tum.i13.server.persistentStorage.btree;

import java.io.IOException;
import java.io.Serializable;

class ChunkStorageMock<V> implements Serializable{

  private static final long serialVersionUID = 6529685098267757691L;

  private Chunk<V> chunk;

  ChunkStorageMock(String filePath) {
  }

  Chunk<V> readChunkFromMemory() throws Exception {    

    return this.chunk.clone();
  }

  // TODO: change exception type;
  void storeChunkInMemory(Chunk<V> chunk) throws IOException {
    this.chunk = chunk;
  }
}
