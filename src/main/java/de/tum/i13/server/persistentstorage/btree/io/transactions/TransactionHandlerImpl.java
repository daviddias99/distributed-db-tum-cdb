package de.tum.i13.server.persistentstorage.btree.io.transactions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.server.persistentstorage.btree.PersistentBTreeNode;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.server.persistentstorage.btree.io.StorageUtils;
import de.tum.i13.shared.Constants;

/** 
 * An implementation of a {@link TransactionHandler}
 */
public class TransactionHandlerImpl<V> implements TransactionHandler<V> {
  private static final Logger LOGGER = LogManager.getLogger(TransactionHandlerImpl.class);

  private static final String DEFAULT_DIRECTORY = "bckp";

  // These fields are static because, reading nodes from file leads to new
  // references being created which caused nodes to access different transaction
  // handlers. This way, the datastructure-references remain constant for all.
  private static Set<String> changedChunks; // chunks that changed since the beginning of the transaction
  private static Set<String> createdChunks; // chunks created since the beggining of the transaction
  private static boolean transactionStarted;

  private String storageFolder;
  private String backupFolder = DEFAULT_DIRECTORY;

  /**
   * Create a new transaction handler
   * @param storageFolder folder where chunks are stored
   * @param backupFolder folder where backup chunks are stored
   */
  public TransactionHandlerImpl(String storageFolder, String backupFolder) {
    TransactionHandlerImpl.changedChunks = new HashSet<>();
    TransactionHandlerImpl.createdChunks = new HashSet<>();
    this.storageFolder = storageFolder;
    this.backupFolder = backupFolder;
    TransactionHandlerImpl.transactionStarted = false;
  }

  /**
   * Create a new transaction handler
   * @param storageFolder folder where chunks are stored
   */
  public TransactionHandlerImpl(String storageFolder) {
    this(storageFolder, DEFAULT_DIRECTORY);
  }

  @Override
  public void notifyChunkChange(String chunkId) throws StorageException {
    if (!transactionStarted) {
      return;
    }

    // Make a copy if it's an existant (before the start of transaction) chunk that
    // is changing.
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

  @Override
  public void notifyChunkCreation(String chunkId) {
    if (!transactionStarted) {
      return;
    }

    createdChunks.add(chunkId);
  }

  @Override
  public PersistentBTreeNode<V> rollbackTransaction() throws StorageException {

    if (!transactionStarted) {
      this.endTransaction();
      return null;
    }

    // Replace changed chunks with previous versions
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

    // Delete newly created chunks
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

    // Replace tree structure
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

    // Read new root and return it
    PersistentBTreeNode<V> newRoot = (PersistentBTreeNode<V>) StorageUtils
        .readObject(Paths.get(this.storageFolder, "root"));
    return newRoot;
  }

  @Override
  public void beginTransaction() throws StorageException {

    if (transactionStarted) {
      return;
    }

    transactionStarted = true;

    // Create backup directory
    StorageUtils.createDirectory(Paths.get(this.storageFolder, this.backupFolder));
    try {
      // Copy tree root
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
    if (!transactionStarted) {
      return;
    }

    transactionStarted = false;
    changedChunks = new HashSet<>();
    createdChunks = new HashSet<>();

    // Delete backup directory
    StorageUtils.deleteDirectory(Paths.get(this.storageFolder, this.backupFolder).toFile());
  }
}
