package de.tum.i13.server.persistentstorage.btree.storage;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.shared.Constants;

public class TransactionHandler implements Serializable {
  private static final Logger LOGGER = LogManager.getLogger(TransactionHandler.class);

  private static final String DEFAULT_DIRECTORY = "bckp";
  private String storageFolder;
  private Set<String> changedChunks;
  private Set<String> createdChunks;
  private String backupFolder = DEFAULT_DIRECTORY;

  public TransactionHandler(String storageFolder, String backupFolder) {
    changedChunks = new HashSet<>();
    createdChunks = new HashSet<>();
    this.storageFolder = storageFolder;
    this.backupFolder = backupFolder;
  }

  public TransactionHandler(String storageFolder) {
    this(storageFolder, DEFAULT_DIRECTORY);
  }

  public void notifyChunkChange(String chunkId) throws StorageException {

    if (changedChunks.add(chunkId)) {
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
    createdChunks.add(chunkId);
  }

  public void rollbackTransaction() throws StorageException {

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

    for (String chunkId : changedChunks) {
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
      StorageUtils.copyAndReplaceFile(Paths.get(this.storageFolder, this.backupFolder, "root"), Paths.get(this.storageFolder, "root"));
    } catch (IOException e) {
      StorageException ex = new StorageException(e, "An error occured during rollback");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, ex);
      this.endTransaction();
      throw ex;
    }
    this.endTransaction();
  }

  public void beginTransaction() throws StorageException {
    StorageUtils.createDirectory(Paths.get(this.storageFolder, this.backupFolder));
    try {
      StorageUtils.copyAndReplaceFile(Paths.get(this.storageFolder, "root"), Paths.get(this.storageFolder, this.backupFolder, "root"));
    } catch (IOException e) {
      StorageException ex = new StorageException(e, "An error occured during begin transaction");
      LOGGER.error(Constants.THROWING_EXCEPTION_LOG_MESSAGE, ex);
      this.endTransaction();
      throw ex;
    }
  }

  public void endTransaction() {
    StorageUtils.deleteDirectory(Paths.get(this.storageFolder, this.backupFolder).toFile());
  }
}
