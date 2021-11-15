package de.tum.i13.server.persistentStorage.btree;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

class ChunkStorageHandler<V> implements Serializable{

  private static final long serialVersionUID = 6529685098267757691L;

  private String filePath;

  ChunkStorageHandler(String filePath) {
    this.filePath = filePath;
  }

  Chunk<V> readChunkFromMemory() throws Exception {

    FileInputStream fileIn = new FileInputStream(this.filePath);
    ObjectInputStream objectIn = new ObjectInputStream(fileIn);

    @SuppressWarnings("unchecked")
    DatabaseChunk<V> chunk = (DatabaseChunk<V>) objectIn.readObject();
    objectIn.close();

    return chunk;
  }

  // TODO: change exception type;
  void storeChunkInMemory(Chunk<V> chunk) throws IOException {
    FileOutputStream fileOut = new FileOutputStream(this.filePath);
    var objectOut = new ObjectOutputStream(fileOut);
    objectOut.writeObject(chunk);
    objectOut.close();
  }
}
