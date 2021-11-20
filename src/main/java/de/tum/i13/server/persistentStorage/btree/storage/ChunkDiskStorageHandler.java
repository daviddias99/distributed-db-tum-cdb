package de.tum.i13.server.persistentStorage.btree.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentStorage.btree.chunk.DatabaseChunk;


public class ChunkDiskStorageHandler<V> implements ChunkStorageHandler<V>, Serializable {

  private static final long serialVersionUID = 6529685098267757691L;

  private String filePath;

  public ChunkDiskStorageHandler(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public Chunk<V> readChunkFromMemory() throws StorageException {

    try {
      FileInputStream fileIn = new FileInputStream(this.filePath);
      ObjectInputStream objectIn = new ObjectInputStream(fileIn);

      @SuppressWarnings("unchecked")
      DatabaseChunk<V> chunk = (DatabaseChunk<V>) objectIn.readObject();
      objectIn.close();
      return chunk;

    } catch (FileNotFoundException e) {
      throw new StorageException(e, "Throwing exception because the file %s could not be found.", this.filePath);
    } catch (IOException e) {
      throw new StorageException(e, "I/O error while reading chunk from memory");
    } catch (ClassNotFoundException e) {
      throw new StorageException(e, "Unknown error while reading chunk from memory");
    }
  }

  // TODO: change exception type;
  @Override
  public void storeChunkInMemory(Chunk<V> chunk) throws StorageException {

    if (chunk.getKeyCount() == 0) {
      this.deleteChunk();
      return;
    }

    this.storeChunkInMemoryForce(chunk);
  }

  @Override
  public void storeChunkInMemoryForce(Chunk<V> chunk) throws StorageException {
    try {
      FileOutputStream fileOut = new FileOutputStream(this.filePath);
      var objectOut = new ObjectOutputStream(fileOut);
      objectOut.writeObject(chunk);
      objectOut.close();

    } catch (FileNotFoundException e) {
      throw new StorageException(e, "Throwing exception because the file %s could not be found.", this.filePath);
    } catch (IOException e) {
      throw new StorageException(e, "I/O error while writing chunk to memory");
    } 
  }


  private void deleteChunk() {
    File file = new File(this.filePath);
    file.delete();
  }
}
