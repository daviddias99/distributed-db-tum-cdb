package de.tum.i13.server.persistentstorage.btree.storage.transactions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.persistentstorage.btree.PersistentBTreeNode;
import de.tum.i13.server.persistentstorage.btree.storage.StorageException;
import de.tum.i13.server.persistentstorage.btree.storage.StorageUtils;
import de.tum.i13.shared.Constants;

public class TransactionHandlerImpl<V> implements TransactionHandler<V> {
  private static final Logger LOGGER = LogManager.getLogger(TransactionHandlerImpl.class);

  private static final String DEFAULT_DIRECTORY = "bckp";
  private String storageFolder;
  private static Set<String> changedChunks;
  private static Set<String> createdChunks;
  private String backupFolder = DEFAULT_DIRECTORY;
  private static boolean transactionStarted;

  public TransactionHandlerImpl(String storageFolder, String backupFolder) {
    TransactionHandlerImpl.changedChunks = new HashSet<>();
    TransactionHandlerImpl.createdChunks = new HashSet<>();
    this.storageFolder = storageFolder;
    this.backupFolder = backupFolder;
    TransactionHandlerImpl.transactionStarted = false;
  }

  public TransactionHandlerImpl(String storageFolder) {
    this(storageFolder, DEFAULT_DIRECTORY);
  }

  public void notifyChunkChange(String chunkId) throws StorageException {

    if (!transactionStarted) {
      return;
    }

    if (!createdChunks.contains(chunkId) && changedChunks.add(chunkId)) {
      Path dst = Paths.get(this.storageFolder, this.backupFolder, chunkId);
      Path src = Paths.get(this.storageFolder, chunkId);

      try {
        StorageUtils.copyAndReplaceFile(src, dst);

      } catch (IOException e) {
        StorageException ex = new StorageException(e, "An error occured during rollback");
        LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, ex);
        this.endTransaction();
        throw ex;
      }
    }
  }

  public void notifyChunkCreation(String chunkId) {
    if (!transactionStarted) {
      return;
    }

    createdChunks.add(chunkId);
  }

  public PersistentBTreeNode<V> rollbackTransaction() throws StorageException {

    if (!transactionStarted) {
      this.endTransaction();
      return null;
    }

    for (String chunkId : changedChunks) {
      Path src = Paths.get(this.storageFolder, this.backupFolder, chunkId);
      Path dst = Paths.get(this.storageFolder, chunkId);
      try {
        StorageUtils.copyAndReplaceFile(src, dst);
      } catch (IOException e) {
        StorageException ex = new StorageException(e, "An error occured during rollback");
        LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, ex);
        this.endTransaction();
        throw ex;
      }
    }

    for (String chunkId : createdChunks) {
      try {
        Files.delete(Paths.get(this.storageFolder, chunkId));
      } catch (IOException e) {
        StorageException ex = new StorageException(e,
            "An error occured while deleting newly created chunk on rollback");
        LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, ex);
        this.endTransaction();
        throw ex;
      }
    }

    try {
      StorageUtils.copyAndReplaceFile(Paths.get(this.storageFolder, this.backupFolder, "root"),
          Paths.get(this.storageFolder, "root"));
    } catch (IOException e) {
      StorageException ex = new StorageException(e, "An error occured during rollback");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, ex);
      this.endTransaction();
      throw ex;
    }

    this.endTransaction();
    @SuppressWarnings("unchecked")
    PersistentBTreeNode<V> newRoot = (PersistentBTreeNode<V>) StorageUtils.readObject(Paths.get(this.storageFolder, "root"));
    return newRoot;
  }

  public void beginTransaction() throws StorageException {
    transactionStarted = true;

    StorageUtils.createDirectory(Paths.get(this.storageFolder, this.backupFolder));
    try {
      StorageUtils.copyAndReplaceFile(Paths.get(this.storageFolder, "root"),
          Paths.get(this.storageFolder, this.backupFolder, "root"));
    } catch (IOException e) {
      StorageException ex = new StorageException(e, "An error occured during begin transaction");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, ex);
      this.endTransaction();
      throw ex;
    }
  }

  public void endTransaction() {
    transactionStarted = false;
    changedChunks = new HashSet<>();
    createdChunks = new HashSet<>();
    StorageUtils.deleteDirectory(Paths.get(this.storageFolder, this.backupFolder).toFile());
  }
}
