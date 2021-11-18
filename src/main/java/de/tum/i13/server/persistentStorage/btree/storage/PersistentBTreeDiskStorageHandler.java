package de.tum.i13.server.persistentStorage.btree.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import de.tum.i13.server.persistentStorage.btree.PersistentBTree;
import de.tum.i13.server.persistentStorage.btree.chunk.storage.ChunkDiskStorageHandler;
import de.tum.i13.server.persistentStorage.btree.chunk.storage.ChunkStorageHandler;

public class PersistentBTreeDiskStorageHandler<V> implements PersistentBTreeStorageHandler<V>, Serializable {

  private static final long serialVersionUID = 6523685098267757691L;

  private String filePath;
  private String storageFolder;

  public PersistentBTreeDiskStorageHandler(String storageFolder, boolean reset) {
    this.filePath = storageFolder + "/root";
    this.storageFolder = storageFolder;

    if (reset) {
      this.deleteDirectory(new File(storageFolder));
    }
    this.createDirectory(storageFolder);
  }

  public PersistentBTreeDiskStorageHandler(String storageFolder) {
    this(storageFolder, false);
  }

  private void createDirectory(String storageFolder) {
    File theDir = new File(storageFolder);
    if (!theDir.exists()) {
      theDir.mkdirs();
    }
  }

  private boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }

  public void save(PersistentBTree<V> tree) {
    try {
      FileOutputStream fileOut = new FileOutputStream(this.filePath);
      var objectOut = new ObjectOutputStream(fileOut);
      objectOut.writeObject(tree);
      objectOut.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public PersistentBTree<V> load() {
    try {
      FileInputStream fileIn = new FileInputStream(this.filePath);
      ObjectInputStream objectIn = new ObjectInputStream(fileIn);
      @SuppressWarnings("unchecked")
      PersistentBTree<V> tree = (PersistentBTree<V>) objectIn.readObject();
      objectIn.close();
      return tree;
    } catch (FileNotFoundException e) {
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  public ChunkStorageHandler<V> createChunkStorageHandler(String chunkId) {
    return new ChunkDiskStorageHandler<>(this.storageFolder + "/" + chunkId);
  }
}
