package de.tum.i13.server.persistentStorage.btree;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

class ChunkProxy<V> implements Chunk<V> {
  private DatabaseChunk<V> chunk;
  private String fileName;
  private int lastIndexRead = -1;

  public ChunkProxy(String fileName, int minimumDegree, List<Pair<V>> newElements) throws Exception { 
    this.fileName = fileName;

    DatabaseChunk<V> newChunk = new DatabaseChunk<V>(minimumDegree, newElements);
    this.chunk = newChunk;
    this.storeChunkInMemory();
    this.deleteChunk();
  }

  public ChunkProxy(String fileName, int minimumDegree) throws Exception {
    this.fileName = fileName;

    DatabaseChunk<V> newChunk = new DatabaseChunk<V>(minimumDegree);
    this.chunk = newChunk;
    this.storeChunkInMemory();
    this.deleteChunk();
  }

  // TODO: change exception type;
  private void readChunkFromMemory() throws Exception {

    FileInputStream fileIn = new FileInputStream(this.fileName);
    ObjectInputStream objectIn = new ObjectInputStream(fileIn);

    @SuppressWarnings("unchecked")
    DatabaseChunk<V> chunk = (DatabaseChunk<V>) objectIn.readObject();
    objectIn.close();

    this.chunk = chunk;
  }

  void deleteChunk() {
    this.chunk = null;
  }

  // TODO: change exception type;
  void storeChunkInMemory() throws IOException {
    FileOutputStream fileOut = new FileOutputStream(this.fileName);
    ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
    objectOut.writeObject(chunk);
    objectOut.close();
  }

  @Override
  public int findIndexOfFirstGreaterThen(String k) {
    int result = -1;
    try {
      this.readChunkFromMemory();
      result = this.chunk.findIndexOfFirstGreaterThen(k);
    } catch (Exception e) {
      e.printStackTrace();
    }
    this.deleteChunk();

    return result;
  }

  @Override
  public Pair<V> findValueOfFirstGreaterThen(String k) {
    Pair<V> result = null;
    try {
      this.readChunkFromMemory();
      result = this.chunk.findValueOfFirstGreaterThen(k);
    } catch (Exception e) {
      e.printStackTrace();
    }
    this.deleteChunk();

    return result;
  }

  @Override
  public Pair<V> get(int index) {
    Pair<V> result = null;
    try {
      this.readChunkFromMemory();
      result = this.chunk.get(index);
      this.lastIndexRead = index;
    } catch (Exception e) {
      e.printStackTrace();
    }
    this.deleteChunk();

    return result;
  }

  @Override
  public Pair<V> set(int index, Pair<V> element) {
    Pair<V> result = null;
    try {
      this.readChunkFromMemory();
      result = this.chunk.set(index, element);
      this.storeChunkInMemory();
    } catch (Exception e) {
      e.printStackTrace();
    }

    this.deleteChunk();
    return result;
  }

  @Override
  public int lastIndexRead() {
    return this.lastIndexRead;
  }

  @Override
  public void shiftRightOne(int startIndex) {
    try {
      this.readChunkFromMemory();
      chunk.shiftRightOne(startIndex);
      this.storeChunkInMemory();
    } catch (Exception e) {
      e.printStackTrace();
    }

    this.deleteChunk();
  }

  @Override
  public Pair<V> remove(int index) {
    Pair<V> result = null;
    try {
      this.readChunkFromMemory();
      result = this.chunk.remove(index);
      this.storeChunkInMemory();
    } catch (Exception e) {
      e.printStackTrace();
    }

    this.deleteChunk();
    return result;
  }

  @Override
  public int shiftRightOneAfterFirstGreaterThan(String key) {
    int i = -1;
    try {
      this.readChunkFromMemory();
      i = chunk.shiftRightOneAfterFirstGreaterThan(key);
      this.storeChunkInMemory();
    } catch (Exception e) {
      e.printStackTrace();
    }

    this.deleteChunk();

    return i;
  }
}
