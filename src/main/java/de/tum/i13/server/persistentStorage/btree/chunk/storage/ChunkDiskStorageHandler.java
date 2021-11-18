package de.tum.i13.server.persistentStorage.btree.chunk.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentStorage.btree.chunk.DatabaseChunk;

public class ChunkDiskStorageHandler<V> implements ChunkStorageHandler<V>, Serializable{

  private static final long serialVersionUID = 6529685098267757691L;

  private String filePath;

  public ChunkDiskStorageHandler(String filePath) {
    this.filePath = filePath;
  }

  public Chunk<V> readChunkFromMemory() throws Exception {

    FileInputStream fileIn = new FileInputStream(this.filePath);
    ObjectInputStream objectIn = new ObjectInputStream(fileIn);

    @SuppressWarnings("unchecked")
    DatabaseChunk<V> chunk = (DatabaseChunk<V>) objectIn.readObject();
    objectIn.close();

    return chunk;
  }

  // TODO: change exception type;
  public void storeChunkInMemory(Chunk<V> chunk) throws IOException {
    FileOutputStream fileOut = new FileOutputStream(this.filePath);
    var objectOut = new ObjectOutputStream(fileOut);
    objectOut.writeObject(chunk);
    objectOut.close();
  }
}
