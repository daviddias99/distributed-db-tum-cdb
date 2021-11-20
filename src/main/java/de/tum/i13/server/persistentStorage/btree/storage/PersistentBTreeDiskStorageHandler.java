package de.tum.i13.server.persistentStorage.btree.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.persistentStorage.btree.PersistentBTree;
import de.tum.i13.shared.Constants;

/**
 * Implements {@link ChunkStorageHandler} by storing chunks of type
 * {@link PersistentBTree} on disk.
 */
public class PersistentBTreeDiskStorageHandler<V> implements PersistentBTreeStorageHandler<V>, Serializable {
  private static final Logger LOGGER = LogManager.getLogger(ChunkDiskStorageHandler.class);
  private static final long serialVersionUID = 6523685098267757691L;
  
  private String filePath; // Tree structure file path
  private String storageFolder; // Tree and chunks storage folder

  /**
   * Create a new storage handler which will store a tree in {@code storageFolder}
   * 
   * @param storageFolder Folder where the tree and it's chunks will be stored
   * @param reset         True if the target folder should be cleared if it
   *                      already exists.
   * @throws StorageException An exception is thrown when an error with the {@code storageFolder} occurs
   */
  public PersistentBTreeDiskStorageHandler(String storageFolder, boolean reset) throws StorageException {
    this.filePath = storageFolder + "/root";
    this.storageFolder = storageFolder;

    if (reset) {
      this.deleteDirectory(new File(storageFolder));
    }
    this.createDirectory(storageFolder);
  }

  /**
   * Create a new storage handler which will store a tree in {@code storageFolder}.
   * 
   * @param storageFolder Folder where the tree and it's chunks will be stored
   * @throws StorageException An exception is thrown when an error with the {@code storageFolder} occurs
   */
  public PersistentBTreeDiskStorageHandler(String storageFolder) throws StorageException {
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

  @Override
  public void save(PersistentBTree<V> tree) throws StorageException {
    try {
      FileOutputStream fileOut = new FileOutputStream(this.filePath);
      var objectOut = new ObjectOutputStream(fileOut);
      objectOut.writeObject(tree);
      objectOut.close();
      LOGGER.debug("Stored tree ({}) in disk.", this.filePath);
    } catch (FileNotFoundException e) {
      StorageException storageException = new StorageException(e, "Throwing exception because the file %s could not be found.", this.filePath);
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    } catch (IOException e) {
      StorageException storageException = new StorageException(e, "I/O error while writing tree to disk");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    } 
  }

  @Override
  public PersistentBTree<V> load() throws StorageException {
    try {
      FileInputStream fileIn = new FileInputStream(this.filePath);
      ObjectInputStream objectIn = new ObjectInputStream(fileIn);
      @SuppressWarnings("unchecked")
      PersistentBTree<V> tree = (PersistentBTree<V>) objectIn.readObject();
      objectIn.close();
      LOGGER.debug("Loaded tree ({}) from disk.", this.filePath);
      return tree;
    } catch (FileNotFoundException e) {
      StorageException storageException = new StorageException(e, "Throwing exception because the file %s could not be found.", this.filePath);
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    } catch (IOException e) {
      StorageException storageException = new StorageException(e, "I/O error while reading tree from memory");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    } catch (ClassNotFoundException e) {
      StorageException storageException = new StorageException(e, "Unknown error while reading tree from memory");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    }
  }

  @Override
  public ChunkStorageHandler<V> createChunkStorageHandler(String chunkId) {
    return new ChunkDiskStorageHandler<>(this.storageFolder + "/" + chunkId);
  }

  @Override
  public void delete() throws StorageException {

    if (!this.deleteDirectory(new File(storageFolder))) {
      StorageException storageException = new StorageException("Unknown error while reading tree from memory");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, storageException);
      throw storageException;
    } 
  }
}
