package de.tum.i13.server.persistentStorage.btree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

class TreeStorageHandler<V> implements Serializable{

  private static final long serialVersionUID = 6523685098267757691L;

  private String filePath;

  TreeStorageHandler(String storageFolder, boolean reset) {
    this.filePath = storageFolder + "/root";

    if(reset) {
      this.deleteDirectory(new File(storageFolder));
    }

  }

  boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
        for (File file : allContents) {
            deleteDirectory(file);
        }
    }
    return directoryToBeDeleted.delete();
}

  void saveToDisk(PersistentBtree<V> tree) {
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

  PersistentBtree<V> readFromDisk() {
    try {
      FileInputStream fileIn = new FileInputStream(this.filePath);
      ObjectInputStream objectIn = new ObjectInputStream(fileIn);
      @SuppressWarnings("unchecked")
      PersistentBtree<V> tree = (PersistentBtree<V>) objectIn.readObject();
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
}
